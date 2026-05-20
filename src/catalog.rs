use crate::{
    pool::{configure_connection, configure_database, DatabasePool},
    EsError, Result,
};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use turso::Database;

fn get_text_safe(row: &turso::Row, index: usize) -> Result<String> {
    row.get_value(index)?
        .as_text()
        .ok_or_else(|| EsError::Migration(format!("Expected text value at column {}", index)))
        .map(|s| s.to_string())
}

fn get_integer_safe(row: &turso::Row, index: usize) -> Result<i64> {
    row.get_value(index)?
        .as_integer()
        .ok_or_else(|| EsError::Migration(format!("Expected integer value at column {}", index)))
        .copied()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionRef {
    pub name: String,
    pub path: String,
    pub first_version: i64,
    pub last_version: Option<i64>,
    pub sealed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnerLogHead {
    pub current_version: i64,
    pub last_event_id: Option<uuid::Uuid>,
    pub active_partition: Option<String>,
}

pub struct Catalog {
    db: Database,
    pool: Option<Arc<DatabasePool>>,
}

impl Catalog {
    pub async fn open(path: &Path) -> Result<Self> {
        let db_path = path.join("catalog.db");
        let db_path_str = db_path.to_str().ok_or_else(|| {
            EsError::InvalidPath("Catalog path contains invalid UTF-8".to_string())
        })?;

        let db = turso::Builder::new_local(db_path_str).build().await?;
        configure_database(&db).await?;

        let conn = db.connect()?;
        configure_connection(&conn).await?;
        crate::migration::catalog_migrations().run(&conn).await?;

        Ok(Self { db, pool: None })
    }

    pub async fn open_with_pool(path: &Path, pool: Arc<DatabasePool>) -> Result<Self> {
        let db_path = path.join("catalog.db");
        let db_path_str = db_path.to_str().ok_or_else(|| {
            EsError::InvalidPath("Catalog path contains invalid UTF-8".to_string())
        })?;

        let db = turso::Builder::new_local(db_path_str).build().await?;
        configure_database(&db).await?;

        {
            let conn = pool.get_catalog_connection().await?;
            crate::migration::catalog_migrations().run(&conn).await?;
        }

        Ok(Self {
            db,
            pool: Some(pool),
        })
    }

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

    pub async fn create_partition(&self, partition: &PartitionRef) -> Result<()> {
        let sealed = if partition.sealed { 1 } else { 0 };
        let conn = self.get_connection().await?;
        conn.execute(
            r#"
            INSERT INTO partitions (name, path, first_version, last_version, sealed)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(name) DO UPDATE SET
                path = excluded.path,
                first_version = excluded.first_version,
                last_version = excluded.last_version,
                sealed = excluded.sealed
            "#,
            (
                partition.name.clone(),
                partition.path.clone(),
                partition.first_version,
                partition.last_version,
                sealed,
            ),
        )
        .await?;
        Ok(())
    }

    pub async fn seal_partition(&self, name: &str, last_version: i64) -> Result<()> {
        let conn = self.get_connection().await?;
        conn.execute(
            "UPDATE partitions SET sealed = 1, last_version = ?1 WHERE name = ?2",
            (last_version, name),
        )
        .await?;
        Ok(())
    }

    pub async fn get_active_partition(&self) -> Result<Option<PartitionRef>> {
        let conn = self.get_connection().await?;
        let mut rows = conn
            .query(
                "SELECT name, path, first_version, last_version, sealed FROM partitions WHERE sealed = 0 ORDER BY first_version DESC, name DESC LIMIT 1",
                (),
            )
            .await?;

        let Some(row) = rows.next().await? else {
            return Ok(None);
        };

        Ok(Some(self.row_to_partition_ref(&row)?))
    }

    pub async fn get_all_partitions(&self) -> Result<Vec<PartitionRef>> {
        let conn = self.get_connection().await?;
        let mut rows = conn
            .query(
                "SELECT name, path, first_version, last_version, sealed FROM partitions ORDER BY first_version, name",
                (),
            )
            .await?;
        self.collect_partitions_from_rows(&mut rows).await
    }

    pub async fn get_partitions_after_version(&self, version: i64) -> Result<Vec<PartitionRef>> {
        let conn = self.get_connection().await?;
        let mut rows = conn
            .query(
                r#"
                SELECT name, path, first_version, last_version, sealed
                FROM partitions
                WHERE last_version IS NULL OR last_version > ?1
                ORDER BY first_version, name
                "#,
                (version,),
            )
            .await?;
        self.collect_partitions_from_rows(&mut rows).await
    }

    pub async fn get_head(&self) -> Result<OwnerLogHead> {
        let conn = self.get_connection().await?;
        let mut rows = conn
            .query(
                "SELECT current_version, last_event_id, active_partition FROM owner_log WHERE id = 1",
                (),
            )
            .await?;

        let Some(row) = rows.next().await? else {
            return Ok(OwnerLogHead {
                current_version: 0,
                last_event_id: None,
                active_partition: None,
            });
        };

        let last_event_id = self
            .get_optional_text(&row, 1)?
            .map(|s| uuid::Uuid::parse_str(&s))
            .transpose()?;

        Ok(OwnerLogHead {
            current_version: get_integer_safe(&row, 0)?,
            last_event_id,
            active_partition: self.get_optional_text(&row, 2)?,
        })
    }

    pub async fn update_head(
        &self,
        current_version: i64,
        last_event_id: &uuid::Uuid,
        active_partition: &str,
    ) -> Result<()> {
        let conn = self.get_connection().await?;
        conn.execute(
            r#"
            INSERT INTO owner_log (id, current_version, last_event_id, active_partition)
            VALUES (1, ?1, ?2, ?3)
            ON CONFLICT(id) DO UPDATE SET
                current_version = excluded.current_version,
                last_event_id = excluded.last_event_id,
                active_partition = excluded.active_partition
            WHERE excluded.current_version >= owner_log.current_version
            "#,
            (current_version, last_event_id.to_string(), active_partition),
        )
        .await?;
        Ok(())
    }

    fn row_to_partition_ref(&self, row: &turso::Row) -> Result<PartitionRef> {
        Ok(PartitionRef {
            name: get_text_safe(row, 0)?,
            path: get_text_safe(row, 1)?,
            first_version: get_integer_safe(row, 2)?,
            last_version: self.get_optional_integer(row, 3)?,
            sealed: get_integer_safe(row, 4)? == 1,
        })
    }

    fn get_optional_integer(&self, row: &turso::Row, index: usize) -> Result<Option<i64>> {
        let value = row.get_value(index)?;
        Ok(value.as_integer().copied())
    }

    fn get_optional_text(&self, row: &turso::Row, index: usize) -> Result<Option<String>> {
        let value = row.get_value(index)?;
        Ok(value.as_text().map(|s| s.to_string()))
    }

    async fn collect_partitions_from_rows(
        &self,
        rows: &mut turso::Rows,
    ) -> Result<Vec<PartitionRef>> {
        let mut partitions = Vec::new();
        while let Some(row) = rows.next().await? {
            partitions.push(self.row_to_partition_ref(&row)?);
        }
        Ok(partitions)
    }
}
