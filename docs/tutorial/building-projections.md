# Building Projections and Read Models

In a CQRS system, the write model (events) is optimized for capturing intent, while the read model (projections) is optimized for querying. This tutorial teaches you how to build efficient read models from your event streams.

## What You'll Learn

- What projections are and why you need them
- How to create basic and advanced projections
- Implementing idempotent event processing
- Managing checkpoints and recovery
- Building real-time read models

## Prerequisites

- Completed the [First Project tutorial](first-event-store.md)
- Understanding of SQL databases
- About 30 minutes to complete

## What Are Projections?

A projection is a read model built by processing events from one or more streams. Think of it like this:

```
Events (Write Model)          Projection (Read Model)
┌─────────────────┐          ┌─────────────────┐
│ OrderCreated    │ ──►      │ Orders Table    │
│ ItemAdded       │ ──►      │ Items Table     │
│ PaymentProcessed│ ──►      │ Payments Table  │
│ OrderShipped    │ ──►      │ Status View     │
└─────────────────┘          └─────────────────┘
```

Projections allow you to:

- Query data efficiently (no need to replay events)
- Join data from multiple streams
- Create denormalized views for specific use cases
- Maintain real-time synchronization with events

## Step 1: Set Up the Project Environment

Create a new project:

```bash
cargo new order_projections
cd order_projections
```

Add to `Cargo.toml`:

```toml
[dependencies]
events = "0.1.0"
tokio = { version = "1.0", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
uuid = { version = "1.0", features = ["v4", "serde"] }
chrono = { version = "0.4", features = ["serde"] }
turso = "0.2.0-pre.14"
anyhow = "1.0"
tracing = "0.1"
tracing-subscriber = "0.3"
```

## Step 2: Define Read Model Schemas

Let's define the tables for our read models:

```rust
use turso::{Connection, Result as SqlResult};
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderReadModel {
    pub order_id: String,
    pub customer_id: String,
    pub customer_email: String,
    pub status: String,
    pub total_amount: i64,
    pub item_count: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderItemReadModel {
    pub id: String,
    pub order_id: String,
    pub product_id: String,
    pub quantity: i32,
    pub unit_price: i64,
    pub total_price: i64,
    pub added_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderSummaryReadModel {
    pub order_id: String,
    pub customer_id: String,
    pub status: String,
    pub total_amount: i64,
    pub last_updated: DateTime<Utc>,
}

pub struct ProjectionDatabase {
    conn: Connection,
}

impl ProjectionDatabase {
    pub fn new(database_path: &str) -> SqlResult<Self> {
        let conn = Connection::open(database_path)?;

        // Enable foreign keys and WAL mode for better performance
        conn.execute("PRAGMA foreign_keys = ON", [])?;
        conn.execute("PRAGMA journal_mode = WAL", [])?;

        let db = Self { conn };
        db.init_tables()?;
        Ok(db)
    }

    fn init_tables(&self) -> SqlResult<()> {
        // Orders table
        self.conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                customer_email TEXT NOT NULL,
                status TEXT NOT NULL,
                total_amount INTEGER NOT NULL DEFAULT 0,
                item_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            "#,
            [],
        )?;

        // Order items table
        self.conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS order_items (
                id TEXT PRIMARY KEY,
                order_id TEXT NOT NULL,
                product_id TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                unit_price INTEGER NOT NULL,
                total_price INTEGER NOT NULL,
                added_at TEXT NOT NULL,
                FOREIGN KEY (order_id) REFERENCES orders (order_id) ON DELETE CASCADE
            )
            "#,
            [],
        )?;

        // Applied events table (for idempotency)
        self.conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS applied_events (
                event_id TEXT PRIMARY KEY,
                applied_at TEXT NOT NULL
            )
            "#,
            [],
        )?;

        // Create indexes for better query performance
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders (customer_id)", [])?;
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status)", [])?;
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items (order_id)", [])?;
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items (product_id)", [])?;

        Ok(())
    }
}
```

## Step 3: Create the Basic Projector

Let's implement a basic projector that processes order events:

```rust
use events::{EventStore, Projector, bootstrap_cursor, EsError, EventEnvelope};
use anyhow::Result;

pub struct OrderProjector {
    store: EventStore,
    db: ProjectionDatabase,
    projection_name: String,
}

impl OrderProjector {
    pub fn new(store: EventStore, db_path: &str) -> Result<Self> {
        let db = ProjectionDatabase::new(db_path)?;
        Ok(Self {
            store,
            db,
            projection_name: "order_projection".to_string(),
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        println!("🚀 Starting order projection...");

        // Bootstrap cursor from last processed position
        let cursor = bootstrap_cursor(&self.store, &self.projection_name).await?;

        let projector = Projector::new(self.store.clone(), self.projection_name.clone())
            .with_batch_size(100);

        println!("📍 Starting from cursor: {:?}", cursor);

        // Run the projector
        projector.run(|events| async move {
            self.process_batch(events).await
        }).await?;

        Ok(())
    }

    async fn process_batch(&mut self, events: Vec<EventEnvelope>) -> Result<(), EsError> {
        println!("Processing batch of {} events", events.len());

        // Process events within a transaction
        let tx = self.db.conn.transaction()?;

        for event in &events {
            // Check if event was already processed (idempotency)
            if self.is_event_already_applied(&tx, &event.id)? {
                println!("Skipping already processed event: {}", event.id);
                continue;
            }

            // Process the event based on its type
            match event.r#type.as_str() {
                "OrderCreated" => self.handle_order_created(&tx, event)?,
                "OrderItemAdded" => self.handle_order_item_added(&tx, event)?,
                "OrderItemRemoved" => self.handle_order_item_removed(&tx, event)?,
                "OrderConfirmed" => self.handle_order_confirmed(&tx, event)?,
                "PaymentProcessed" => self.handle_payment_processed(&tx, event)?,
                "OrderShipped" => self.handle_order_shipped(&tx, event)?,
                "OrderDelivered" => self.handle_order_delivered(&tx, event)?,
                "OrderCancelled" => self.handle_order_cancelled(&tx, event)?,
                _ => {
                    println!("Unknown event type: {}", event.r#type);
                }
            }

            // Mark event as applied
            self.mark_event_applied(&tx, &event.id)?;
        }

        // Commit the transaction
        tx.commit()?;
        println!("Batch processed successfully");

        Ok(())
    }

    fn is_event_already_applied(&self, conn: &turso::Connection, event_id: &str) -> SqlResult<bool> {
        let mut stmt = conn.prepare(
            "SELECT 1 FROM applied_events WHERE event_id = ?"
        )?;

        let exists = stmt.exists([event_id])?;
        Ok(exists)
    }

    fn mark_event_applied(&self, conn: &turso::Connection, event_id: &str) -> SqlResult<()> {
        conn.execute(
            "INSERT OR IGNORE INTO applied_events (event_id, applied_at) VALUES (?, ?)",
            [event_id, &Utc::now().to_rfc3339()]
        )?;
        Ok(())
    }
}
```

## Step 4: Implement Event Handlers

Now let's implement the specific handlers for each event type:

```rust
use serde_json::Value;

// Add these methods to OrderProjector implementation

impl OrderProjector {
    fn handle_order_created(&self, conn: &turso::Connection, event: &EventEnvelope) -> SqlResult<()> {
        let payload: Value = serde_json::from_str(&event.payload)?;

        let order_id = payload["order_id"].as_str().unwrap();
        let customer_id = payload["customer"]["customer_id"].as_str().unwrap();
        let customer_email = payload["customer"]["email"].as_str().unwrap();
        let created_at = payload["created_at"].as_str().unwrap();

        // Calculate initial total from items
        let mut total_amount = 0i64;
        let mut item_count = 0i32;

        if let Some(items) = payload["items"].as_array() {
            for item in items {
                let quantity = item["quantity"].as_u64().unwrap() as i32;
                let unit_price = item["unit_price"].as_i64().unwrap();
                total_amount += quantity as i64 * unit_price;
                item_count += quantity;
            }
        }

        conn.execute(
            r#"
            INSERT INTO orders (
                order_id, customer_id, customer_email, status,
                total_amount, item_count, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            [
                order_id,
                customer_id,
                customer_email,
                "Created",
                &total_amount.to_string(),
                &item_count.to_string(),
                created_at,
                created_at,
            ],
        )?;

        // Insert initial items
        if let Some(items) = payload["items"].as_array() {
            for (index, item) in items.iter().enumerate() {
                let item_id = format!("{}-{}", order_id, index);
                let product_id = item["product_id"].as_str().unwrap();
                let quantity = item["quantity"].as_u64().unwrap() as i32;
                let unit_price = item["unit_price"].as_i64().unwrap();
                let total_price = quantity as i64 * unit_price;

                conn.execute(
                    r#"
                    INSERT INTO order_items (
                        id, order_id, product_id, quantity, unit_price, total_price, added_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    "#,
                    [
                        &item_id,
                        order_id,
                        product_id,
                        &quantity.to_string(),
                        &unit_price.to_string(),
                        &total_price.to_string(),
                        created_at,
                    ],
                )?;
            }
        }

        println!("✅ Created order read model: {}", order_id);
        Ok(())
    }

    fn handle_order_item_added(&self, conn: &turso::Connection, event: &EventEnvelope) -> SqlResult<()> {
        let payload: Value = serde_json::from_str(&event.payload)?;

        let order_id = payload["order_id"].as_str().unwrap();
        let product_id = payload["item"]["product_id"].as_str().unwrap();
        let quantity = payload["item"]["quantity"].as_u64().unwrap() as i32;
        let unit_price = payload["item"]["unit_price"].as_i64().unwrap();
        let added_at = payload["added_at"].as_str().unwrap();

        let total_price = quantity as i64 * unit_price;

        // Add the item
        let item_id = format!("{}-{}", order_id, uuid::Uuid::new_v4());
        conn.execute(
            r#"
            INSERT INTO order_items (
                id, order_id, product_id, quantity, unit_price, total_price, added_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
            [
                &item_id,
                order_id,
                product_id,
                &quantity.to_string(),
                &unit_price.to_string(),
                &total_price.to_string(),
                added_at,
            ],
        )?;

        // Update order totals
        conn.execute(
            r#"
            UPDATE orders
            SET total_amount = total_amount + ?,
                item_count = item_count + ?,
                updated_at = ?
            WHERE order_id = ?
            "#,
            [
                &total_price.to_string(),
                &quantity.to_string(),
                added_at,
                order_id,
            ],
        )?;

        println!("➕ Added item to order {}: {} x {}", order_id, quantity, product_id);
        Ok(())
    }

    fn handle_order_item_removed(&self, conn: &turso::Connection, event: &EventEnvelope) -> SqlResult<()> {
        let payload: Value = serde_json::from_str(&event.payload)?;

        let order_id = payload["order_id"].as_str().unwrap();
        let product_id = payload["product_id"].as_str().unwrap();
        let quantity_removed = payload["quantity"].as_u64().unwrap() as i32;
        let removed_at = payload["removed_at"].as_str().unwrap();

        // Find and remove items (removing from oldest first)
        let mut stmt = conn.prepare(
            "SELECT id, quantity, unit_price FROM order_items
             WHERE order_id = ? AND product_id = ?
             ORDER BY added_at ASC"
        )?;

        let rows = stmt.query_map([order_id, product_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i32>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;

        let mut remaining_to_remove = quantity_removed;
        let mut total_refund = 0i64;

        for row in rows {
            let (item_id, quantity, unit_price) = row?;

            if remaining_to_remove <= 0 {
                break;
            }

            let remove_quantity = quantity.min(remaining_to_remove);
            let refund = remove_quantity as i64 * unit_price;
            total_refund += refund;

            if remove_quantity == quantity {
                // Remove the entire item row
                conn.execute("DELETE FROM order_items WHERE id = ?", [&item_id])?;
            } else {
                // Update the quantity
                let new_quantity = quantity - remove_quantity;
                let new_total_price = new_quantity as i64 * unit_price;
                conn.execute(
                    "UPDATE order_items SET quantity = ?, total_price = ? WHERE id = ?",
                    [&new_quantity.to_string(), &new_total_price.to_string(), &item_id]
                )?;
            }

            remaining_to_remove -= remove_quantity;
        }

        // Update order totals
        conn.execute(
            r#"
            UPDATE orders
            SET total_amount = total_amount - ?,
                item_count = item_count - ?,
                updated_at = ?
            WHERE order_id = ?
            "#,
            [
                &total_refund.to_string(),
                &quantity_removed.to_string(),
                removed_at,
                order_id,
            ],
        )?;

        println!("➖ Removed {} x {} from order {}", quantity_removed, product_id, order_id);
        Ok(())
    }

    fn handle_order_confirmed(&self, conn: &turso::Connection, event: &EventEnvelope) -> SqlResult<()> {
        let payload: Value = serde_json::from_str(&event.payload)?;
        let order_id = payload["order_id"].as_str().unwrap();
        let confirmed_at = payload["confirmed_at"].as_str().unwrap();

        conn.execute(
            "UPDATE orders SET status = 'Confirmed', updated_at = ? WHERE order_id = ?",
            [confirmed_at, order_id]
        )?;

        println!("✅ Confirmed order: {}", order_id);
        Ok(())
    }

    fn handle_payment_processed(&self, conn: &turso::Connection, event: &EventEnvelope) -> SqlResult<()> {
        let payload: Value = serde_json::from_str(&event.payload)?;
        let order_id = payload["order_id"].as_str().unwrap();
        let processed_at = payload["processed_at"].as_str().unwrap();

        conn.execute(
            "UPDATE orders SET status = 'Paid', updated_at = ? WHERE order_id = ?",
            [processed_at, order_id]
        )?;

        println!("💳 Payment processed for order: {}", order_id);
        Ok(())
    }

    fn handle_order_shipped(&self, conn: &turso::Connection, event: &EventEnvelope) -> SqlResult<()> {
        let payload: Value = serde_json::from_str(&event.payload)?;
        let order_id = payload["order_id"].as_str().unwrap();
        let shipped_at = payload["shipped_at"].as_str().unwrap();

        conn.execute(
            "UPDATE orders SET status = 'Shipped', updated_at = ? WHERE order_id = ?",
            [shipped_at, order_id]
        )?;

        println!("🚚 Order shipped: {}", order_id);
        Ok(())
    }

    fn handle_order_delivered(&self, conn: &turso::Connection, event: &EventEnvelope) -> SqlResult<()> {
        let payload: Value = serde_json::from_str(&event.payload)?;
        let order_id = payload["order_id"].as_str().unwrap();
        let delivered_at = payload["delivered_at"].as_str().unwrap();

        conn.execute(
            "UPDATE orders SET status = 'Delivered', updated_at = ? WHERE order_id = ?",
            [delivered_at, order_id]
        )?;

        println!("📦 Order delivered: {}", order_id);
        Ok(())
    }

    fn handle_order_cancelled(&self, conn: &turso::Connection, event: &EventEnvelope) -> SqlResult<()> {
        let payload: Value = serde_json::from_str(&event.payload)?;
        let order_id = payload["order_id"].as_str().unwrap();
        let cancelled_at = payload["cancelled_at"].as_str().unwrap();

        conn.execute(
            "UPDATE orders SET status = 'Cancelled', updated_at = ? WHERE order_id = ?",
            [cancelled_at, order_id]
        )?;

        println!("❌ Order cancelled: {}", order_id);
        Ok(())
    }
}
```

## Step 5: Add Query Methods

Let's add methods to query our read models:

```rust
impl OrderProjector {
    pub fn get_order(&self, order_id: &str) -> SqlResult<Option<OrderReadModel>> {
        let mut stmt = self.db.conn.prepare(
            r#"
            SELECT order_id, customer_id, customer_email, status,
                   total_amount, item_count, created_at, updated_at
            FROM orders WHERE order_id = ?
            "#
        )?;

        let order = stmt.query_row([order_id], |row| {
            Ok(OrderReadModel {
                order_id: row.get(0)?,
                customer_id: row.get(1)?,
                customer_email: row.get(2)?,
                status: row.get(3)?,
                total_amount: row.get(4)?,
                item_count: row.get(5)?,
                created_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(6)?)
                    .unwrap()
                    .with_timezone(&Utc),
                updated_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(7)?)
                    .unwrap()
                    .with_timezone(&Utc),
            })
        }).optional()?;

        Ok(order)
    }

    pub fn get_orders_by_customer(&self, customer_id: &str) -> SqlResult<Vec<OrderReadModel>> {
        let mut stmt = self.db.conn.prepare(
            r#"
            SELECT order_id, customer_id, customer_email, status,
                   total_amount, item_count, created_at, updated_at
            FROM orders WHERE customer_id = ?
            ORDER BY created_at DESC
            "#
        )?;

        let orders = stmt.query_map([customer_id], |row| {
            Ok(OrderReadModel {
                order_id: row.get(0)?,
                customer_id: row.get(1)?,
                customer_email: row.get(2)?,
                status: row.get(3)?,
                total_amount: row.get(4)?,
                item_count: row.get(5)?,
                created_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(6)?)
                    .unwrap()
                    .with_timezone(&Utc),
                updated_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(7)?)
                    .unwrap()
                    .with_timezone(&Utc),
            })
        })?;

        let result: SqlResult<Vec<_>> = orders.collect();
        result
    }

    pub fn get_orders_by_status(&self, status: &str) -> SqlResult<Vec<OrderSummaryReadModel>> {
        let mut stmt = self.db.conn.prepare(
            r#"
            SELECT order_id, customer_id, status, total_amount, updated_at
            FROM orders WHERE status = ?
            ORDER BY updated_at DESC
            "#
        )?;

        let orders = stmt.query_map([status], |row| {
            Ok(OrderSummaryReadModel {
                order_id: row.get(0)?,
                customer_id: row.get(1)?,
                status: row.get(2)?,
                total_amount: row.get(3)?,
                last_updated: DateTime::parse_from_rfc3339(&row.get::<_, String>(4)?)
                    .unwrap()
                    .with_timezone(&Utc),
            })
        })?;

        let result: SqlResult<Vec<_>> = orders.collect();
        result
    }

    pub fn get_order_items(&self, order_id: &str) -> SqlResult<Vec<OrderItemReadModel>> {
        let mut stmt = self.db.conn.prepare(
            r#"
            SELECT id, order_id, product_id, quantity, unit_price, total_price, added_at
            FROM order_items WHERE order_id = ?
            ORDER BY added_at ASC
            "#
        )?;

        let items = stmt.query_map([order_id], |row| {
            Ok(OrderItemReadModel {
                id: row.get(0)?,
                order_id: row.get(1)?,
                product_id: row.get(2)?,
                quantity: row.get(3)?,
                unit_price: row.get(4)?,
                total_price: row.get(5)?,
                added_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(6)?)
                    .unwrap()
                    .with_timezone(&Utc),
            })
        })?;

        let result: SqlResult<Vec<_>> = items.collect();
        result
    }

    pub fn get_projection_stats(&self) -> SqlResult<ProjectionStats> {
        let total_orders: i64 = self.db.conn.query_row(
            "SELECT COUNT(*) FROM orders",
            [],
            |row| row.get(0)
        )?;

        let total_events: i64 = self.db.conn.query_row(
            "SELECT COUNT(*) FROM applied_events",
            [],
            |row| row.get(0)
        )?;

        let status_counts = self.db.conn.prepare(
            "SELECT status, COUNT(*) FROM orders GROUP BY status"
        )?;

        let status_distribution: SqlResult<Vec<_>> = status_counts.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?.collect();

        Ok(ProjectionStats {
            total_orders,
            total_events,
            status_distribution: status_distribution?,
        })
    }
}

#[derive(Debug)]
pub struct ProjectionStats {
    pub total_orders: i64,
    pub total_events: i64,
    pub status_distribution: Vec<(String, i64)>,
}
```

## Step 6: Create the Main Application

Let's create a complete application that demonstrates the projection system:

```rust
use events::{EventStore, RotationPolicy};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Create event store
    let event_store = EventStore::open_partitioned(
        "./event_data",
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600),
            max_bytes: Some(512 * 1024 * 1024),
        },
    ).await?;

    // Create projector
    let mut projector = OrderProjector::new(event_store, "./projections.db")?;

    println!("🎯 Order Projection System");
    println!("==========================");

    // Start the projection
    projector.start().await?;

    // Display projection statistics
    let stats = projector.get_projection_stats()?;
    println!("\n📊 Projection Statistics:");
    println!("   Total Orders: {}", stats.total_orders);
    println!("   Total Events Processed: {}", stats.total_events);
    println!("   Status Distribution:");
    for (status, count) in &stats.status_distribution {
        println!("     {}: {}", status, count);
    }

    // Example queries
    println!("\n🔍 Example Queries:");

    // Get a specific order
    if let Some(order) = projector.get_order("example-order-id")? {
        println!("\nOrder Details:");
        println!("   ID: {}", order.order_id);
        println!("   Customer: {}", order.customer_email);
        println!("   Status: {}", order.status);
        println!("   Total: ${:.2}", order.total_amount as f64 / 100.0);
        println!("   Items: {}", order.item_count);

        // Get order items
        let items = projector.get_order_items(&order.order_id)?;
        for item in items {
            println!("     - {} x {} @ ${:.2}",
                item.quantity, item.product_id, item.unit_price as f64 / 100.0);
        }
    }

    // Get orders by status
    let pending_orders = projector.get_orders_by_status("Pending")?;
    println!("\nPending Orders: {}", pending_orders.len());

    // Get customer orders
    let customer_orders = projector.get_orders_by_customer("customer-123")?;
    println!("Customer Orders: {}", customer_orders.len());

    Ok(())
}
```

## Step 7: Advanced Features

### Multi-Projection Support

Let's add support for running multiple projections:

```rust
pub struct ProjectionManager {
    projections: Vec<Box<dyn Projection>>,
}

impl ProjectionManager {
    pub fn new() -> Self {
        Self {
            projections: Vec::new(),
        }
    }

    pub fn add_projection(&mut self, projection: Box<dyn Projection>) {
        self.projections.push(projection);
    }

    pub async fn start_all(&mut self) -> Result<(), EsError> {
        for projection in &mut self.projections {
            projection.start().await?;
        }
        Ok(())
    }
}

pub trait Projection {
    fn start(&mut self) -> Result<(), EsError>;
    fn name(&self) -> &str;
    fn stats(&self) -> ProjectionStats;
}
```

### Snapshot Support

For long-lived aggregates, add snapshots to avoid replaying all events:

```rust
impl OrderProjector {
    pub fn create_snapshot(&self, order_id: &str) -> SqlResult<()> {
        if let Some(order) = self.get_order(order_id)? {
            let snapshot = OrderSnapshot {
                order_id: order.order_id.clone(),
                data: serde_json::to_value(&order)?,
                version: self.get_current_event_version(order_id)?,
                created_at: Utc::now(),
            };

            // Store snapshot
            self.db.conn.execute(
                "INSERT OR REPLACE INTO order_snapshots (order_id, data, version, created_at) VALUES (?, ?, ?, ?)",
                [
                    &snapshot.order_id,
                    &serde_json::to_string(&snapshot.data)?,
                    &snapshot.version.to_string(),
                    &snapshot.created_at.to_rfc3339(),
                ]
            )?;
        }
        Ok(())
    }

    fn load_from_snapshot(&self, order_id: &str) -> SqlResult<Option<OrderReadModel>> {
        let mut stmt = self.db.conn.prepare(
            "SELECT data FROM order_snapshots WHERE order_id = ? ORDER BY created_at DESC LIMIT 1"
        )?;

        let snapshot: Option<String> = stmt.query_row([order_id], |row| row.get(0)).optional()?;

        if let Some(data) = snapshot {
            let order: OrderReadModel = serde_json::from_str(&data)?;
            Ok(Some(order))
        } else {
            Ok(None)
        }
    }
}
```

## Best Practices

### 1. Idempotency

Always track which events have been processed to handle duplicates and restarts.

### 2. Transactional Consistency

Process events in batches within transactions to maintain consistency.

### 3. Error Handling

Implement proper error recovery and retry mechanisms.

### 4. Monitoring

Track projection lag and processing statistics.

### 5. Performance

- Use appropriate batch sizes
- Create database indexes
- Consider materialized views for complex queries

## What We've Built

You've successfully created a complete projection system that includes:

✅ **Read Model Management**: Efficient querying of order data
✅ **Idempotent Processing**: Safe handling of event duplicates
✅ **Real-time Updates**: Automatic synchronization with events
✅ **Query Optimization**: Fast read access through denormalization
✅ **Error Recovery**: Robust handling of failures and restarts

## Next Steps

Ready to learn more? Check out these guides:

- [How to Configure Rotation](../how-to/configure-rotation.md) - Optimize your event store performance
- [How to Monitor Production](../how-to/monitor-production.md) - Keep your projections healthy
- [How to Scale Consumers](../how-to/scale-consumers.md) - Handle high-volume event streams

## Common Issues and Solutions

**Issue**: Projection lag behind events
**Solution**: Increase batch size, optimize queries, or scale horizontally

**Issue**: Duplicate data in read models
**Solution**: Ensure proper idempotency checks and transaction boundaries

**Issue**: Slow query performance
**Solution**: Add appropriate indexes, consider materialized views, or denormalize further

**Issue**: Memory usage with large projections
**Solution**: Implement streaming processing and consider snapshot strategies

