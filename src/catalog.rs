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
pub struct EventFileRange {
    pub name: String,
    pub path: String,
    pub first_version: i64,
    pub last_version: Option<i64>,
    pub sealed: bool,
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

    pub async fn create_event_file_range(&self, range: &EventFileRange) -> Result<()> {
        let sealed = if range.sealed { 1 } else { 0 };
        let conn = self.get_connection().await?;
        conn.execute(
            r#"
            INSERT INTO event_file_ranges (name, path, first_version, last_version, sealed)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(name) DO UPDATE SET
                path = excluded.path,
                first_version = excluded.first_version,
                last_version = excluded.last_version,
                sealed = excluded.sealed
            "#,
            (
                range.name.clone(),
                range.path.clone(),
                range.first_version,
                range.last_version,
                sealed,
            ),
        )
        .await?;
        Ok(())
    }

    pub async fn seal_event_file_range(&self, name: &str, last_version: i64) -> Result<()> {
        let conn = self.get_connection().await?;
        conn.execute(
            "UPDATE event_file_ranges SET sealed = 1, last_version = ?1 WHERE name = ?2",
            (last_version, name),
        )
        .await?;
        Ok(())
    }

    pub async fn get_active_event_file_range(&self) -> Result<Option<EventFileRange>> {
        let conn = self.get_connection().await?;
        let mut rows = conn
            .query(
                "SELECT name, path, first_version, last_version, sealed FROM event_file_ranges WHERE sealed = 0 ORDER BY first_version DESC, name DESC LIMIT 1",
                (),
            )
            .await?;

        let Some(row) = rows.next().await? else {
            return Ok(None);
        };

        Ok(Some(self.row_to_event_file_range(&row)?))
    }

    pub async fn replace_event_file_ranges(&self, ranges: &[EventFileRange]) -> Result<()> {
        let conn = self.get_connection().await?;
        conn.execute("BEGIN IMMEDIATE", ()).await?;
        if let Err(e) = conn.execute("DELETE FROM event_file_ranges", ()).await {
            let _ = conn.execute("ROLLBACK", ()).await;
            return Err(e.into());
        }

        for range in ranges {
            let sealed = if range.sealed { 1 } else { 0 };
            if let Err(e) = conn
                .execute(
                    r#"
                    INSERT INTO event_file_ranges (name, path, first_version, last_version, sealed)
                    VALUES (?1, ?2, ?3, ?4, ?5)
                    "#,
                    (
                        range.name.clone(),
                        range.path.clone(),
                        range.first_version,
                        range.last_version,
                        sealed,
                    ),
                )
                .await
            {
                let _ = conn.execute("ROLLBACK", ()).await;
                return Err(e.into());
            }
        }

        if let Err(e) = conn.execute("COMMIT", ()).await {
            let _ = conn.execute("ROLLBACK", ()).await;
            return Err(e.into());
        }
        Ok(())
    }

    pub async fn get_all_event_file_ranges(&self) -> Result<Vec<EventFileRange>> {
        let conn = self.get_connection().await?;
        let mut rows = conn
            .query(
                "SELECT name, path, first_version, last_version, sealed FROM event_file_ranges ORDER BY first_version, name",
                (),
            )
            .await?;
        self.collect_event_file_ranges_from_rows(&mut rows).await
    }

    pub async fn get_event_file_ranges_after_version(
        &self,
        version: i64,
    ) -> Result<Vec<EventFileRange>> {
        let conn = self.get_connection().await?;
        let mut rows = conn
            .query(
                r#"
                SELECT name, path, first_version, last_version, sealed
                FROM event_file_ranges
                WHERE last_version IS NULL OR last_version > ?1
                ORDER BY first_version, name
                "#,
                (version,),
            )
            .await?;
        self.collect_event_file_ranges_from_rows(&mut rows).await
    }

    fn row_to_event_file_range(&self, row: &turso::Row) -> Result<EventFileRange> {
        Ok(EventFileRange {
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

    async fn collect_event_file_ranges_from_rows(
        &self,
        rows: &mut turso::Rows,
    ) -> Result<Vec<EventFileRange>> {
        let mut ranges = Vec::new();
        while let Some(row) = rows.next().await? {
            ranges.push(self.row_to_event_file_range(&row)?);
        }
        Ok(ranges)
    }
}
