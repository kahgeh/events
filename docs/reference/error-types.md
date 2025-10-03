# Error Types Reference

Comprehensive guide to error handling in the Events crate, including all error types, causes, and recovery strategies.

## Error Hierarchy

The Events crate uses a single `EsError` enum that encompasses all possible error conditions:

```rust
pub enum EsError {
    Db(turso::Error),                             // Database-related errors
    Concurrency { expected: i64, actual: i64, stream_id: String }, // Concurrency conflicts
    PayloadTooLarge { size: usize, max: usize },  // Event payload size violations
    Serde(serde_json::Error),                    // Serialization/deserialization errors
    Uuid(uuid::Error),                           // UUID generation/parsing errors
    Time(time::error::ComponentRange),           // Time-related errors
    Io(std::io::Error),                          // File system I/O errors
    Migration(String),                           // Schema migration errors
    InvalidPartition(String),                    // Invalid partition operations
    Cursor(String),                              // Cursor-related errors
    InvalidPath(String),                         // Invalid file system paths
    InvalidTableName(String),                    // Table name validation errors
}
```

## Error Categories

### 1. Database Errors (`EsError::Db`)

Database errors are the most common and typically indicate issues with Turso operations.

#### Common Causes

```rust
// Database locked
EsError::Db(/* turso database busy error */)

// Connection issues
EsError::Db(/* turso connection error */)

// Query execution failed
EsError::Db(/* turso query error */)
```

#### Recovery Strategies

```rust
match store.append(stream_id, expected_version, events).await {
    Err(EsError::Db(db_error)) => {
        // Check if it's a retryable database error
        if db_error.to_string().contains("busy") || db_error.to_string().contains("locked") {
            // Retry with exponential backoff
            tokio::time::sleep(Duration::from_millis(100)).await;
            // Retry the operation
        } else {
            return Err(EsError::Db(db_error));
        }
    }
    result => result,
}
```

#### Prevention

```rust
// Use connection pooling
let pool = DatabasePool::new("./data")?
    .with_max_cached_databases(20);

// Enable WAL mode for better concurrency
async fn configure_wal_mode(conn: &turso::Connection) -> Result<(), EsError> {
    conn.execute("PRAGMA journal_mode = WAL", ()).await?;
    conn.execute("PRAGMA synchronous = NORMAL", ()).await?;
    conn.execute("PRAGMA busy_timeout = 30000", ()).await?; // 30 second timeout
    Ok(())
}
```

### 2. Concurrency Errors (`EsError::Concurrency`)

Concurrency errors occur when multiple processes try to modify the same stream simultaneously.

#### Structure

```rust
pub struct ConcurrencyError {
    pub expected: i64,      // Expected stream version
    pub actual: i64,        // Actual stream version
    pub stream_id: String,  // Stream identifier
}
```

#### Common Scenarios

```rust
// Process A reads version 5
let events_a = store.load("order-123").await?; // Returns version 5

// Process B reads version 5
let events_b = store.load("order-123").await?; // Returns version 5

// Process A appends (succeeds, new version 6)
let result_a = store.append("order-123", ExpectedVersion::Exact(5), events_a).await?;

// Process B appends (fails, stream is now version 6)
let result_b = store.append("order-123", ExpectedVersion::Exact(5), events_b).await?;
// Result: Err(EsError::Concurrency { expected: 5, actual: 6, stream_id: "order-123" })
```

#### Recovery Strategies

```rust
async fn append_with_retry(
    store: &EventStore,
    stream_id: &str,
    events: Vec<NewEvent>,
    max_retries: u32,
) -> Result<AppendResult, EsError> {
    let mut retries = 0;

    loop {
        // Load current state
        let current_events = store.load(stream_id).await?;
        let expected_version = if current_events.is_empty() {
            ExpectedVersion::NoStream
        } else {
            ExpectedVersion::Exact(current_events.len() as i64)
        };

        match store.append(stream_id, expected_version, events.clone()).await {
            Ok(result) => return Ok(result),
            Err(EsError::Concurrency { actual, .. }) => {
                retries += 1;
                if retries >= max_retries {
                    return Err(EsError::Concurrency {
                        expected: match expected_version {
                            ExpectedVersion::Exact(v) => v,
                            _ => -1,
                        },
                        actual,
                        stream_id: stream_id.to_string(),
                    });
                }

                // Exponential backoff with jitter
                let base_delay = 100 * 2_u64.pow(retries - 1);
                let jitter = rand::random::<u64>() % (base_delay / 4);
                let delay = Duration::from_millis(base_delay + jitter);

                tokio::time::sleep(delay).await;
            }
            Err(e) => return Err(e),
        }
    }
}
```

#### Prevention

```rust
// Use shorter transaction scopes
async fn append_events_atomic(
    store: &EventStore,
    stream_id: &str,
    events: Vec<NewEvent>,
) -> Result<AppendResult, EsError> {
    // Get version and append in quick succession
    let current_events = store.load(stream_id).await?;
    let expected = ExpectedVersion::Exact(current_events.len() as i64);

    store.append(stream_id, expected, events).await
}

// Use event merging for non-critical updates
async fn merge_events_if_possible(
    store: &EventStore,
    stream_id: &str,
    new_events: Vec<NewEvent>,
) -> Result<AppendResult, EsError> {
    match store.append(stream_id, ExpectedVersion::Any, new_events).await {
        Ok(result) => Ok(result),
        Err(EsError::Concurrency { .. }) => {
            // Merge with existing state and retry
            let current_events = store.load(stream_id).await?;
            let merged_events = merge_event_streams(&current_events, &new_events)?;
            store.append(stream_id, ExpectedVersion::Exact(current_events.len() as i64), merged_events).await
        }
        Err(e) => Err(e),
    }
}
```

### 3. Payload Size Errors (`EsError::PayloadTooLarge`)

These errors occur when event payloads exceed the configured size limit.

#### Structure

```rust
pub struct PayloadTooLargeError {
    pub size: usize,    // Actual payload size in bytes
    pub max: usize,     // Maximum allowed size in bytes
}
```

#### Configuration

```rust
// Note: EventStore::with_config doesn't exist in current implementation
// Use the standard constructor for now:
let store = EventStore::open_partitioned("./data", rotation_policy).await?;
// Future versions may support custom configuration like:
// EventStore::with_config(EventStoreConfig {
//     max_payload_bytes: 10 * 1024 * 1024, // 10MB limit
//     ..Default::default()
// }).await?;
```

#### Recovery Strategies

```rust
async fn append_large_payload_safely(
    store: &EventStore,
    stream_id: &str,
    large_payload: serde_json::Value,
) -> Result<(), EsError> {
    // Check payload size before attempting append
    let payload_str = serde_json::to_string(&large_payload)?;
    let payload_size = payload_str.len();

    if payload_size > store.max_payload_size() {
        // Strategy 1: Compress the payload
        if let Ok(compressed) = compress_payload(&payload_str) {
            if compressed.len() <= store.max_payload_size() {
                let compressed_event = NewEvent {
                    r#type: "CompressedEvent".to_string(),
                    payload: json!({
                        "compression": "gzip",
                        "data": base64::encode(&compressed)
                    }),
                };
                return store.append(stream_id, ExpectedVersion::Any, vec![compressed_event]).await.map(drop);
            }
        }

        // Strategy 2: Split into multiple events
        let split_events = split_large_payload(large_payload)?;
        return store.append(stream_id, ExpectedVersion::Any, split_events).await.map(drop);
    }

    // Normal append
    let event = NewEvent {
        r#type: "LargeEvent".to_string(),
        payload: large_payload,
    };
    store.append(stream_id, ExpectedVersion::Any, vec![event]).await.map(drop)
}
```

#### Prevention

```rust
fn validate_event_payload(event: &NewEvent, max_size: usize) -> Result<(), EsError> {
    let payload_str = serde_json::to_string(&event.payload)
        .map_err(EsError::Serde)?;

    if payload_str.len() > max_size {
        return Err(EsError::PayloadTooLarge {
            size: payload_str.len(),
            max: max_size,
        });
    }

    Ok(())
}

// Validate before creating events
fn create_validated_event(
    event_type: String,
    payload: serde_json::Value,
    max_size: usize,
) -> Result<NewEvent, EsError> {
    let event = NewEvent { r#type: event_type, payload };
    validate_event_payload(&event, max_size)?;
    Ok(event)
}
```

### 4. Serialization Errors (`EsError::Serde`)

These errors occur during JSON serialization or deserialization of event data.

#### Common Causes

```rust
// Invalid JSON in payload
let invalid_json = r#"{"invalid": json}"#;
let payload: serde_json::Value = serde_json::from_str(invalid_json)?;
// Result: Err(EsError::Serde(serde_json::Error::Syntax(...)))

// Non-serializable data types
use std::time::SystemTime;
let event = NewEvent {
    r#type: "Test".to_string(),
    payload: json!({"timestamp": SystemTime::now()}), // SystemTime doesn't serialize
};
```

#### Recovery Strategies

```rust
async fn safe_event_serialization(event_type: &str, data: &dyn Serialize) -> Result<NewEvent, EsError> {
    match serde_json::to_value(data) {
        Ok(payload) => Ok(NewEvent {
            r#type: event_type.to_string(),
            payload,
        }),
        Err(e) => {
            // Fallback: store error information
            Ok(NewEvent {
                r#type: format!("{}SerializationError", event_type),
                payload: json!({
                    "error": e.to_string(),
                    "original_type": event_type,
                    "timestamp": Utc::now()
                }),
            })
        }
    }
}

async fn safe_event_deserialization<T: DeserializeOwned>(
    event: &EventEnvelope
) -> Result<T, EsError> {
    serde_json::from_value(event.payload.clone())
        .map_err(|e| {
            // Log the error and return a structured error
            eprintln!("Failed to deserialize event {} (type: {}): {}",
                event.id, event.r#type, e);
            EsError::Serde(e)
        })
}
```

#### Prevention

```rust
// Use serializable types
#[derive(Serialize, Deserialize)]
pub struct OrderEvent {
    pub order_id: String,
    pub timestamp: chrono::DateTime<chrono::Utc>, // Use chrono instead of SystemTime
    pub amount: i64,
}

// Validate JSON before creating events
fn validate_json(payload: &serde_json::Value) -> Result<(), EsError> {
    serde_json::to_string(payload)
        .map_err(EsError::Serde)
        .map(drop)
}
```

### 5. Partition Errors (`EsError::InvalidPartition`)

These errors occur when attempting operations on invalid or non-existent partitions.

#### Common Causes

```rust
// Accessing non-existent partition
store.load_partition("non_existent_partition").await?;
// Result: Err(EsError::InvalidPartition("Partition not found".to_string()))

// Invalid partition names
store.load_partition("../../../etc/passwd").await?;
// Result: Err(EsError::InvalidPartition("Invalid partition name".to_string()))
```

#### Recovery Strategies

```rust
async fn safe_partition_operation(
    store: &EventStore,
    partition_name: &str,
) -> Result<(), EsError> {
    // Validate partition name
    if !is_valid_partition_name(partition_name) {
        return Err(EsError::InvalidPartition(
            format!("Invalid partition name: {}", partition_name)
        ));
    }

    // Check if partition exists
    if !store.partition_exists(partition_name).await? {
        return Err(EsError::InvalidPartition(
            format!("Partition not found: {}", partition_name)
        ));
    }

    // Perform operation
    Ok(())
}

fn is_valid_partition_name(name: &str) -> bool {
    // Check for path traversal attempts
    if name.contains("..") || name.contains('/') || name.contains('\\') {
        return false;
    }

    // Check for valid pattern (e.g., events_YYYYMMDD.db)
    let pattern = regex::Regex::new(r"^events_\d{8}(_\d{2,4})?(_[a-z])?$").unwrap();
    pattern.is_match(name)
}
```

### 6. Cursor Errors (`EsError::Cursor`)

These errors occur when working with cursors for cross-partition navigation.

#### Common Causes

```rust
// Invalid cursor format
let invalid_cursor = PartitionedCursor {
    partition: "invalid".to_string(),
    created_at_ms: -1,
    event_id: Uuid::new_v4(),
};

// Cursor pointing to deleted event
let deleted_event_cursor = get_cursor_for_deleted_event().await?;
store.all_since(deleted_event_cursor, 100).await?;
// Result: Err(EsError::Cursor("Event not found".to_string()))
```

#### Recovery Strategies

```rust
async fn safe_cursor_navigation(
    store: &EventStore,
    mut cursor: PartitionedCursor,
) -> Result<Vec<EventEnvelope>, EsError> {
    loop {
        match store.all_since(cursor.clone(), 1000).await {
            Ok((events, next_cursor)) => {
                if events.is_empty() {
                    // No events found, try moving to next partition
                    if let Some(next_partition) = store.get_next_partition(&cursor.partition).await? {
                        cursor = PartitionedCursor::new(next_partition, 0, uuid::Uuid::new_v4());
                        continue;
                    } else {
                        return Ok(vec![]); // Reached the end
                    }
                }
                return Ok(events);
            }
            Err(EsError::Cursor(msg)) if msg.contains("not found") => {
                // Event not found, move to beginning of current partition
                cursor = PartitionedCursor::new(cursor.partition.clone(), 0, uuid::Uuid::new_v4());
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}
```

### 7. Time Errors (`EsError::Time`)

These errors occur when dealing with time operations, such as creating timestamps or parsing time values.

#### Common Causes

```rust
// Invalid timestamp ranges
use time::OffsetDateTime;
let dt = OffsetDateTime::from_unix_timestamp_nanos(i128::MAX); // Too large

// Invalid date components
let date = time::Date::from_ymd(2024, 2, 30); // February 30th doesn't exist
```

#### Recovery Strategies

```rust
async fn handle_time_operations() -> Result<(), EsError> {
    // Use current time instead of invalid timestamps
    let now = time::OffsetDateTime::now_utc();

    // Validate date components before creating dates
    fn create_safe_date(year: i32, month: u8, day: u8) -> Result<time::Date, EsError> {
        time::Date::from_calendar_date(
            year,
            time::Month::try_from(month).map_err(|e| EsError::Time(e))?,
            day,
        ).map_err(|e| EsError::Time(e))
    }

    Ok(())
}
```

### 8. Path Errors (`EsError::InvalidPath`)

These errors occur when file system paths are invalid or inaccessible.

#### Common Causes

```rust
// Non-existent directories
let store = EventStore::open_partitioned("/non/existent/path", rotation).await?;

// Invalid UTF-8 in paths
let path = String::from_utf8_lossy(&invalid_bytes);
```

#### Recovery Strategies

```rust
async fn ensure_valid_path(path: &str) -> Result<String, EsError> {
    // Create directory if it doesn't exist
    tokio::fs::create_dir_all(path).await?;

    // Validate path exists and is accessible
    let metadata = tokio::fs::metadata(path).await?;
    if !metadata.is_dir() {
        return Err(EsError::InvalidPath("Path is not a directory".to_string()));
    }

    Ok(path.to_string())
}
```

### 9. Table Name Errors (`EsError::InvalidTableName`)

These errors occur when table names contain invalid characters or are SQL injection attempts.

#### Common Causes

```rust
// SQL injection attempts
let table_name = "users; DROP TABLE events; --";
let processor = IdempotentProcessor::new(conn, table_name.to_string()).await?;

// Invalid characters
let table_name = "users-events"; // Contains hyphen
let table_name = "123users"; // Starts with number
```

#### Recovery Strategies

```rust
async fn create_safe_table_name(base_name: &str) -> Result<String, EsError> {
    // Sanitize the table name
    let sanitized = base_name
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect::<String>();

    // Ensure it starts with a letter
    let safe_name = if sanitized.chars().next().map_or(false, |c| c.is_alphabetic()) {
        sanitized
    } else {
        format!("table_{}", sanitized)
    };

    // Validate with the built-in validator
    crate::validation::TableNameValidator::validate_table_name(&safe_name)
}
```

## Error Handling Best Practices

### 1. Structured Error Handling

```rust
#[derive(Debug, thiserror::Error)]
pub enum ApplicationError {
    #[error("Event store error: {0}")]
    EventStore(#[from] EsError),

    #[error("Business logic error: {message}")]
    Business { message: String },

    #[error("Validation error: {field} - {message}")]
    Validation { field: String, message: String },
}

impl ApplicationError {
    pub fn is_retryable(&self) -> bool {
        match self {
            ApplicationError::EventStore(EsError::Concurrency { .. }) => true,
            ApplicationError::EventStore(EsError::Db(db_err)) => {
            // Check if it's a retryable database error
            db_err.to_string().contains("busy") || db_err.to_string().contains("locked")
        }
            _ => false,
        }
    }

    pub fn error_code(&self) -> &'static str {
        match self {
            ApplicationError::EventStore(EsError::Concurrency { .. }) => "CONCURRENCY_ERROR",
            ApplicationError::EventStore(EsError::Db(_)) => "DATABASE_ERROR",
            ApplicationError::Business { .. } => "BUSINESS_ERROR",
            ApplicationError::Validation { .. } => "VALIDATION_ERROR",
        }
    }
}
```

### 2. Retry Logic

```rust
pub async fn with_retry<F, T, E, Fut>(
    operation: F,
    max_retries: u32,
    base_delay: Duration,
) -> Result<T, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut retries = 0;

    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                retries += 1;
                if retries >= max_retries {
                    return Err(e);
                }

                // Check if error is retryable
                if !is_retryable_error(&e) {
                    return Err(e);
                }

                // Exponential backoff with jitter
                let delay = base_delay * 2_u32.pow(retries - 1);
                let jitter = rand::random::<f64>() * 0.1;
                let actual_delay = Duration::from_millis(
                    (delay.as_millis() as f64 * (1.0 + jitter)) as u64
                );

                tokio::time::sleep(actual_delay).await;
            }
        }
    }
}
```

### 3. Error Logging

```rust
use tracing::{error, warn, debug};

pub fn log_event_store_error(error: &EsError, context: &str) {
    match error {
        EsError::Concurrency { expected, actual, stream_id } => {
            warn!(
                stream_id = %stream_id,
                expected_version = expected,
                actual_version = actual,
                context = %context,
                "Concurrency conflict occurred"
            );
        }
        EsError::Db(db_error) => {
            error!(
                error = %db_error,
                context = %context,
                "Database operation failed"
            );
        }
        EsError::PayloadTooLarge { size, max } => {
            warn!(
                payload_size = size,
                max_size = max,
                context = %context,
                "Event payload too large"
            );
        }
        _ => {
            debug!(
                error = %error,
                context = %context,
                "Event store operation failed"
            );
        }
    }
}
```

### 4. Metrics and Monitoring

```rust
use std::sync::atomic::{AtomicU64, Ordering};

pub struct ErrorMetrics {
    total_errors: AtomicU64,
    concurrency_errors: AtomicU64,
    database_errors: AtomicU64,
    payload_errors: AtomicU64,
    serialization_errors: AtomicU64,
}

impl ErrorMetrics {
    pub fn record_error(&self, error: &EsError) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);

        match error {
            EsError::Concurrency { .. } => self.concurrency_errors.fetch_add(1, Ordering::Relaxed),
            EsError::Db(_) => self.database_errors.fetch_add(1, Ordering::Relaxed),
            EsError::PayloadTooLarge { .. } => self.payload_errors.fetch_add(1, Ordering::Relaxed),
            EsError::Serde(_) => self.serialization_errors.fetch_add(1, Ordering::Relaxed),
            _ => {}
        };
    }

    pub fn get_error_rate(&self, total_operations: u64) -> f64 {
        let errors = self.total_errors.load(Ordering::Relaxed);
        if total_operations > 0 {
            errors as f64 / total_operations as f64
        } else {
            0.0
        }
    }
}
```

## Testing Error Scenarios

```rust
#[cfg(test)]
mod error_tests {
    use super::*;

    #[tokio::test]
    async fn test_concurrency_error_handling() {
        let store = EventStore::open_in_memory().await.unwrap();
        let stream_id = "test-stream";

        // Create initial event
        store.append(stream_id, ExpectedVersion::NoStream, vec![
            NewEvent {
                r#type: "Test".to_string(),
                payload: json!({}),
            }
        ]).await.unwrap();

        // Try concurrent append
        let result = store.append(stream_id, ExpectedVersion::Exact(0), vec![
            NewEvent {
                r#type: "Test".to_string(),
                payload: json!({}),
            }
        ]).await;

        assert!(matches!(result, Err(EsError::Concurrency { .. })));
    }

    #[tokio::test]
    async fn test_payload_size_error() {
        let store = EventStore::open_in_memory().await.unwrap();
        let large_payload = "x".repeat(2 * 1024 * 1024); // 2MB

        let result = store.append("test", ExpectedVersion::NoStream, vec![
            NewEvent {
                r#type: "Large".to_string(),
                payload: json!({"data": large_payload}),
            }
        ]).await;

        assert!(matches!(result, Err(EsError::PayloadTooLarge { .. })));
    }
}
```

## Next Steps

- [API Reference](api.md) - Complete API documentation
- [Configuration Reference](configuration.md) - Configuration options
- [Performance Reference](performance.md) - Performance tuning guide