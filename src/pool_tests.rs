use super::*;
use tempfile::TempDir;

#[tokio::test]
async fn test_database_pool_basic_operations() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let pool = DatabasePool::new(temp_dir.path())?;

    // Create catalog database first
    let catalog_path = temp_dir.path().join("catalog.db");
    let _db = turso::Builder::new_local(catalog_path.to_str().unwrap())
        .build()
        .await?;

    // Test getting catalog connection
    {
        let _conn = pool.get_catalog_connection().await?;
        // Connection is still active here, so we expect 1 active connection
        let stats = pool.stats().await;
        assert_eq!(stats.cached_databases, 1);
        assert_eq!(stats.total_active_connections, 1);
    } // Connection goes out of scope here

    // After dropping the connection, active connections should be 0
    tokio::task::yield_now().await; // Allow drop to complete
    let stats = pool.stats().await;
    assert_eq!(stats.cached_databases, 1);
    assert_eq!(stats.total_active_connections, 0);

    Ok(())
}

#[tokio::test]
async fn test_database_pool_connection_reuse() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let pool = DatabasePool::new(temp_dir.path())?;

    // Create a simple database file
    let db_path = temp_dir.path().join("test.db");
    let db_path_str = db_path.to_str().unwrap();
    let db = turso::Builder::new_local(db_path_str).build().await?;

    // Create a simple table
    let conn = db.connect()?;
    conn.execute("CREATE TABLE test (id INTEGER)", ()).await?;

    // Get connections multiple times
    let conn1 = pool.get_connection("test.db").await?;
    let conn2 = pool.get_connection("test.db").await?;

    // Both connections should work
    conn1
        .execute("INSERT INTO test (id) VALUES (1)", ())
        .await?;
    conn2
        .execute("INSERT INTO test (id) VALUES (2)", ())
        .await?;

    // Check stats
    let stats = pool.stats().await;
    assert_eq!(stats.cached_databases, 1);
    assert_eq!(stats.total_active_connections, 2);

    Ok(())
}
