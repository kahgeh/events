use super::*;
use tempfile::TempDir;

async fn open_connection(temp_dir: &TempDir, name: &str) -> Result<turso::Connection> {
    let path = temp_dir.path().join(name);
    let database = turso::Builder::new_local(path.to_str().expect("test path is valid UTF-8"))
        .build()
        .await?;
    Ok(database.connect()?)
}

async fn query_text(connection: &turso::Connection, sql: &str) -> Result<String> {
    let mut rows = connection.query(sql, ()).await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| EsError::Migration("test query returned no rows".to_string()))?;
    row.get_value(0)?
        .as_text()
        .map(|value| value.to_string())
        .ok_or_else(|| EsError::Migration("test query did not return text".to_string()))
}

#[tokio::test]
async fn fresh_event_file_records_initial_schema() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let connection = open_connection(&temp_dir, "events.db").await?;

    partition_migrations().run(&connection).await?;

    assert_eq!(
        query_text(&connection, "SELECT name FROM _migrations").await?,
        "001_event_file_schema"
    );
    connection.query("SELECT * FROM events", ()).await?;
    connection
        .query("SELECT * FROM event_file_append_head", ())
        .await?;
    Ok(())
}

#[tokio::test]
async fn fresh_catalog_records_initial_schema() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let connection = open_connection(&temp_dir, "catalog.db").await?;

    catalog_migrations().run(&connection).await?;

    assert_eq!(
        query_text(&connection, "SELECT name FROM _migrations").await?,
        "001_catalog_schema"
    );
    connection
        .query("SELECT * FROM event_file_ranges", ())
        .await?;
    Ok(())
}

#[tokio::test]
async fn event_file_baseline_does_not_replace_existing_events_table() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let connection = open_connection(&temp_dir, "events.db").await?;
    connection
        .execute_batch("CREATE TABLE events (marker TEXT); INSERT INTO events VALUES ('keep');")
        .await?;

    assert!(partition_migrations().run(&connection).await.is_err());
    assert_eq!(
        query_text(&connection, "SELECT marker FROM events").await?,
        "keep"
    );
    Ok(())
}

#[tokio::test]
async fn catalog_baseline_does_not_replace_existing_ranges_table() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let connection = open_connection(&temp_dir, "catalog.db").await?;
    connection
        .execute_batch(
            "CREATE TABLE event_file_ranges (marker TEXT); INSERT INTO event_file_ranges VALUES ('keep');",
        )
        .await?;

    assert!(catalog_migrations().run(&connection).await.is_err());
    assert_eq!(
        query_text(&connection, "SELECT marker FROM event_file_ranges").await?,
        "keep"
    );
    Ok(())
}
