use events::{EsError, EventStore, ExpectedVersion, NewEvent, RotationPolicy};
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), EsError> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Create event store with hourly partitions and 1MB size limit
    let store = EventStore::open_partitioned(
        "./data",
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600), // 1 hour
            max_bytes: Some(1024 * 1024),      // 1MB
        },
    )
    .await?;

    println!("Event store opened successfully");

    // Example 1: Publishing events
    let stream_id = "order-123";

    println!("\n=== Publishing Events ===");

    // Check if stream already exists and get current version
    let current_version = store.get_stream_version(stream_id).await?;
    println!(
        "Current stream version for '{}': {}",
        stream_id, current_version
    );

    // First append - use appropriate expected version
    let expected_version = if current_version == 0 {
        println!("Stream '{}' does not exist, creating new stream", stream_id);
        ExpectedVersion::NoStream
    } else {
        println!(
            "Stream '{}' exists with version {}, appending to it",
            stream_id, current_version
        );
        ExpectedVersion::Exact(current_version)
    };

    let result = store
        .append(
            stream_id,
            expected_version,
            vec![
                NewEvent {
                    r#type: "OrderCreated".into(),
                    payload: json!({"sku": "ABC", "qty": 1, "price": 29.99}),
                },
                NewEvent {
                    r#type: "PaymentAuthorized".into(),
                    payload: json!({"amount": 2999, "method": "credit_card"}),
                },
            ],
        )
        .await?;

    println!(
        "Appended {} events, stream version: {}",
        result.events.len(),
        result.version
    );

    // Second append - expect the version we just got from the first append
    let result = store
        .append(
            stream_id,
            ExpectedVersion::Exact(result.version),
            vec![NewEvent {
                r#type: "OrderPacked".into(),
                payload: json!({"warehouse": "W1", "tracking": "TRK123456"}),
            }],
        )
        .await?;

    println!(
        "Appended {} events, stream version: {}",
        result.events.len(),
        result.version
    );

    // Example 2: Loading events
    println!("\n=== Loading Events ===");
    let events = store.load(stream_id).await?;
    println!(
        "Loaded {} events from stream '{}':",
        events.len(),
        stream_id
    );
    for event in &events {
        println!("  - {}: {}", event.r#type, event.payload);
    }

    // Example 3: Simple manual projection
    println!("\n=== Manual Projection Example ===");

    // Load events from all streams using a cursor starting from the beginning
    use events::PartitionedCursor;

    // Get the actual active partition name from the store
    let partition_name = store.get_active_partition_name().await?;

    let cursor = PartitionedCursor {
        partition: partition_name,
        created_at_ms: 0,
        event_id: uuid::Uuid::from_u128(0),
    };

    let (events, _next_cursor) = store.all_since(cursor, 100).await?;
    println!("Projected {} events using cursor", events.len());
    for event in &events {
        match event.r#type.as_str() {
            "OrderCreated" => {
                println!(
                    "Projector: Processing OrderCreated for stream {}",
                    event.stream_id
                );
            }
            "PaymentAuthorized" => {
                println!(
                    "Projector: Processing PaymentAuthorized for stream {}",
                    event.stream_id
                );
            }
            "OrderPacked" => {
                println!(
                    "Projector: Processing OrderPacked for stream {}",
                    event.stream_id
                );
            }
            _ => {}
        }
    }

    // Append more events
    println!("\n=== Publishing More Events ===");
    let stream_id_2 = "order-456";

    // Check if second stream already exists
    let current_version_2 = store.get_stream_version(stream_id_2).await?;
    println!(
        "Current stream version for '{}': {}",
        stream_id_2, current_version_2
    );

    let expected_version_2 = if current_version_2 == 0 {
        println!(
            "Stream '{}' does not exist, creating new stream",
            stream_id_2
        );
        ExpectedVersion::NoStream
    } else {
        println!(
            "Stream '{}' exists with version {}, appending to it",
            stream_id_2, current_version_2
        );
        ExpectedVersion::Exact(current_version_2)
    };

    let result = store
        .append(
            stream_id_2,
            expected_version_2,
            vec![NewEvent {
                r#type: "OrderCreated".into(),
                payload: json!({"sku": "XYZ", "qty": 2, "price": 49.99}),
            }],
        )
        .await?;

    println!("Appended {} events to new stream", result.events.len());

    // Example 4: Rotation
    println!("\n=== Testing Rotation ===");
    store.maybe_rotate().await?;
    println!("Rotation check completed");

    Ok(())
}
