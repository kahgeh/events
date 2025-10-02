use crate::{EsError, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use turso::{Connection, Database};

const JOURNAL_MODE_SQL: &str = "PRAGMA journal_mode = WAL";
const SYNCHRONOUS_SQL: &str = "PRAGMA synchronous = NORMAL";

pub async fn configure_connection(conn: &Connection) -> Result<()> {
    conn.busy_timeout(Duration::from_millis(5_000))?;
    Ok(())
}

pub async fn configure_database(db: &Database) -> Result<()> {
    let conn = db.connect()?;
    configure_connection(&conn).await?;

    let mut rows = conn.query(JOURNAL_MODE_SQL, ()).await?;
    while rows.next().await?.is_some() {}

    conn.execute(SYNCHRONOUS_SQL, ()).await?;
    Ok(())
}

/// A connection pool for managing multiple database instances efficiently
#[derive(Debug)]
pub struct DatabasePool {
    /// Directory root for database files
    root: PathBuf,
    /// Cache of Database instances keyed by database path
    databases: Arc<RwLock<HashMap<String, Arc<Mutex<DatabaseInstance>>>>>,
    /// Maximum number of cached Database instances
    max_cached_databases: usize,
}

/// A wrapper around a Database instance with connection management
#[derive(Debug)]
pub struct DatabaseInstance {
    /// The underlying Database
    db: Database,
    /// Last access timestamp for LRU eviction
    last_access: Arc<Mutex<std::time::Instant>>,
    /// Number of active connections
    active_connections: Arc<AtomicUsize>,
    /// Cached connection for reuse to avoid repeated open/close cycles
    cached_connection: Option<Connection>,
}

impl DatabasePool {
    /// Creates a new database pool with the specified root directory
    ///
    /// # Errors
    ///
    /// Returns an error if the root directory path is invalid
    pub fn new<P: AsRef<Path>>(root: P) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        if !root.exists() {
            return Err(EsError::InvalidPath(
                "Root directory does not exist".to_string(),
            ));
        }

        Ok(Self {
            root,
            databases: Arc::new(RwLock::new(HashMap::new())),
            max_cached_databases: 50, // Default cache size
        })
    }

    /// Sets the maximum number of cached database instances
    pub fn with_max_cached_databases(mut self, max: usize) -> Self {
        self.max_cached_databases = max;
        self
    }

    /// Gets or creates a database instance for the given database path
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The database path is invalid
    /// - The database cannot be created or opened
    pub async fn get_database<P: AsRef<Path>>(
        &self,
        db_path: P,
    ) -> Result<Arc<Mutex<DatabaseInstance>>> {
        let path = db_path.as_ref();

        // If path is already absolute, use it as is. Otherwise, join with root.
        let full_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.root.join(path)
        };

        if !full_path.exists() {
            return Err(EsError::InvalidPath(format!(
                "Database file does not exist: {:?}",
                full_path
            )));
        }

        let path_str = full_path.to_str().ok_or_else(|| {
            EsError::InvalidPath("Database path contains invalid UTF-8".to_string())
        })?;

        let path_key = path_str.to_string();

        // Try to get from cache first
        {
            let databases = self.databases.read().await;
            if let Some(db_instance) = databases.get(&path_key) {
                // Update last access time
                let instance_guard = db_instance.lock().await;
                let mut last_access = instance_guard.last_access.lock().await;
                *last_access = std::time::Instant::now();
                return Ok(Arc::clone(db_instance));
            }
        }

        // Not in cache, create new instance
        let db = turso::Builder::new_local(path_str).build().await?;
        configure_database(&db).await?;

        let db_instance = Arc::new(Mutex::new(DatabaseInstance {
            db,
            last_access: Arc::new(Mutex::new(std::time::Instant::now())),
            active_connections: Arc::new(AtomicUsize::new(0)),
            cached_connection: None,
        }));

        // Add to cache (with eviction if needed)
        {
            let mut databases = self.databases.write().await;

            // Evict old databases if cache is full
            if databases.len() >= self.max_cached_databases {
                self.evict_old_databases(&mut databases).await;
            }

            databases.insert(path_key.clone(), Arc::clone(&db_instance));
        }

        Ok(db_instance)
    }

    /// Gets a connection from the specified database instance
    ///
    /// # Errors
    ///
    /// Returns an error if the connection cannot be established
    pub async fn get_connection<P: AsRef<Path>>(&self, db_path: P) -> Result<PooledConnection> {
        let db_instance = self.get_database(db_path).await?;
        let active_counter = {
            let instance_guard = db_instance.lock().await;
            Arc::clone(&instance_guard.active_connections)
        };

        let mut reused = false;
        let conn: Connection;
        {
            let mut instance_guard = db_instance.lock().await;
            active_counter.fetch_add(1, Ordering::SeqCst);
            if let Some(cached) = instance_guard.cached_connection.take() {
                conn = cached;
                reused = true;
            } else {
                match instance_guard.db.connect() {
                    Ok(new_conn) => {
                        conn = new_conn;
                    }
                    Err(e) => {
                        active_counter.fetch_sub(1, Ordering::SeqCst);
                        return Err(e.into());
                    }
                }
            }
        }

        if !reused {
            if let Err(e) = configure_connection(&conn).await {
                active_counter.fetch_sub(1, Ordering::SeqCst);
                return Err(e);
            }
        }

        Ok(PooledConnection {
            conn: Some(conn),
            active_counter: Some(active_counter),
            instance: Some(Arc::clone(&db_instance)),
        })
    }

    /// Evicts old database instances from the cache using LRU strategy
    async fn evict_old_databases(
        &self,
        databases: &mut HashMap<String, Arc<Mutex<DatabaseInstance>>>,
    ) {
        if databases.len() <= self.max_cached_databases {
            return;
        }

        // Collect instances with their last access times
        let mut instances: Vec<(String, Arc<Mutex<DatabaseInstance>>, std::time::Instant)> =
            Vec::new();

        for (path, db_instance) in databases.iter() {
            let instance_guard = db_instance.lock().await;
            let last_access = *instance_guard.last_access.lock().await;
            instances.push((path.clone(), Arc::clone(db_instance), last_access));
        }

        // Sort by last access time (oldest first)
        instances.sort_by_key(|(_, _, last_access)| *last_access);

        // Evict oldest instances that have no active connections
        let mut evicted = 0;
        let target_evictions = databases.len() - self.max_cached_databases + 1; // Evict one extra to make room

        for (path, db_instance, _) in instances {
            if evicted >= target_evictions {
                break;
            }

            let instance_guard = db_instance.lock().await;
            let active_connections = instance_guard.active_connections.load(Ordering::SeqCst);
            if active_connections == 0 {
                databases.remove(&path);
                evicted += 1;
            }
        }
    }

    /// Gets the catalog database instance
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog database cannot be opened
    pub async fn get_catalog(&self) -> Result<Arc<Mutex<DatabaseInstance>>> {
        self.get_database("catalog.db").await
    }

    /// Gets a connection to the catalog database
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog connection cannot be established
    pub async fn get_catalog_connection(&self) -> Result<PooledConnection> {
        self.get_connection("catalog.db").await
    }

    /// Clears all cached database instances
    pub async fn clear_cache(&self) {
        let mut databases = self.databases.write().await;
        databases.clear();
    }

    /// Gets statistics about the connection pool
    pub async fn stats(&self) -> PoolStats {
        let databases = self.databases.read().await;
        let mut total_active_connections = 0;
        let mut instances = Vec::new();

        for (path, db_instance) in databases.iter() {
            let instance_guard = db_instance.lock().await;
            let active_connections = instance_guard.active_connections.load(Ordering::SeqCst);
            let last_access = *instance_guard.last_access.lock().await;

            total_active_connections += active_connections;
            instances.push(DatabaseInstanceStats {
                path: path.clone(),
                active_connections,
                last_access,
            });
        }

        PoolStats {
            cached_databases: databases.len(),
            max_cached_databases: self.max_cached_databases,
            total_active_connections,
            instances,
        }
    }
}

/// A pooled connection that automatically returns to the pool when dropped
pub struct PooledConnection {
    conn: Option<Connection>,
    active_counter: Option<Arc<AtomicUsize>>,
    instance: Option<Arc<Mutex<DatabaseInstance>>>,
}

impl PooledConnection {
    /// Gets a reference to the underlying connection
    pub fn connection(&self) -> &Connection {
        self.conn
            .as_ref()
            .expect("Connection should always be present")
    }

    /// Consumes the connection and returns it, removing it from the pool
    /// This should be used carefully as it bypasses connection reuse
    pub fn into_inner(mut self) -> Connection {
        self.conn
            .take()
            .expect("Connection should always be present")
    }

    /// Creates a PooledConnection from a direct connection (non-pooled)
    /// This is used for backwards compatibility when no pool is available
    pub fn from_direct(conn: Connection) -> Self {
        Self {
            conn: Some(conn),
            active_counter: None, // No pool management for direct connections
            instance: None,
        }
    }
}

impl std::ops::Deref for PooledConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        self.connection()
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            if let Some(instance) = self.instance.as_ref() {
                if let Ok(mut guard) = instance.try_lock() {
                    guard.cached_connection = Some(conn);
                }
            }
        }

        if let Some(counter) = self.active_counter.take() {
            let _ = counter.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                if current == 0 {
                    None
                } else {
                    Some(current - 1)
                }
            });
        }
    }
}

/// Statistics about the connection pool
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub cached_databases: usize,
    pub max_cached_databases: usize,
    pub total_active_connections: usize,
    pub instances: Vec<DatabaseInstanceStats>,
}

#[derive(Debug, Clone)]
pub struct DatabaseInstanceStats {
    pub path: String,
    pub active_connections: usize,
    pub last_access: std::time::Instant,
}

#[cfg(test)]
mod tests {
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
}
