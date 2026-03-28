# Building a Complete E-commerce Order System

In this tutorial, we'll build a complete e-commerce order management system using the event store. You'll learn how to model complex business logic, handle real-world scenarios, and build robust event-driven applications.

## What We'll Build

A complete order management system with:
- Order creation and management
- Inventory management
- Payment processing
- Order status tracking
- Customer notifications

## Prerequisites

- Completed the [Getting Started tutorial](getting-started.md)
- Understanding of Rust structs and enums
- About 45 minutes to complete

## Project Setup

Create a new Rust project:

```bash
cargo new order_system
cd order_system
```

Add to `Cargo.toml`:

```toml
[dependencies]
events = "0.1.0"
tokio = { version = "1.0", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
uuid = { version = "1.0", features = ["v4", "serde"] }
time = { version = "0.3", features = ["serde"] }
anyhow = "1.0"
```

## Step 1: Define Our Domain Models

Let's start by defining the core types for our order system:

```rust
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use time::OffsetDateTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderItem {
    pub product_id: String,
    pub quantity: u32,
    pub unit_price: i64, // Price in cents
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomerInfo {
    pub customer_id: String,
    pub email: String,
    pub shipping_address: Address,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Address {
    pub street: String,
    pub city: String,
    pub state: String,
    pub postal_code: String,
    pub country: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentInfo {
    pub payment_method_id: String,
    pub amount: i64,
    pub currency: String,
}
```

## Step 2: Define Our Events

Now let's define all the events that can occur in our system:

```rust
use serde_json::Value;
use events::NewEvent;

// Order events
#[derive(Debug, Clone)]
pub struct OrderCreated {
    pub order_id: String,
    pub customer: CustomerInfo,
    pub items: Vec<OrderItem>,
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone)]
pub struct OrderItemAdded {
    pub order_id: String,
    pub item: OrderItem,
    pub added_at: OffsetDateTime,
}

#[derive(Debug, Clone)]
pub struct OrderItemRemoved {
    pub order_id: String,
    pub product_id: String,
    pub quantity: u32,
    pub removed_at: OffsetDateTime,
}

#[derive(Debug, Clone)]
pub struct OrderConfirmed {
    pub order_id: String,
    pub confirmed_at: OffsetDateTime,
}

#[derive(Debug, Clone)]
pub struct PaymentProcessed {
    pub order_id: String,
    pub payment: PaymentInfo,
    pub processed_at: OffsetDateTime,
}

#[derive(Debug, Clone)]
pub struct OrderShipped {
    pub order_id: String,
    pub tracking_number: String,
    pub shipped_at: OffsetDateTime,
}

#[derive(Debug, Clone)]
pub struct OrderDelivered {
    pub order_id: String,
    pub delivered_at: OffsetDateTime,
}

#[derive(Debug, Clone)]
pub struct OrderCancelled {
    pub order_id: String,
    pub reason: String,
    pub cancelled_at: OffsetDateTime,
}
```

## Step 3: Create Event Conversion Helpers

We need helpers to convert our domain events to the `NewEvent` format:

```rust
impl From<OrderCreated> for NewEvent {
    fn from(event: OrderCreated) -> Self {
        NewEvent {
            r#type: "OrderCreated".into(),
            payload: serde_json::to_value(event).unwrap(),
        }
    }
}

impl From<OrderItemAdded> for NewEvent {
    fn from(event: OrderItemAdded) -> Self {
        NewEvent {
            r#type: "OrderItemAdded".into(),
            payload: serde_json::to_value(event).unwrap(),
        }
    }
}

// Add similar implementations for all other events...
```

## Step 4: Build the Order Service

Now let's create a service to manage orders:

```rust
use events::{EventStore, ExpectedVersion, EsError};
use std::sync::Arc;

pub struct OrderService {
    store: Arc<EventStore>,
}

impl OrderService {
    pub fn new(store: Arc<EventStore>) -> Self {
        Self { store }
    }

    pub async fn create_order(
        &self,
        customer: CustomerInfo,
        items: Vec<OrderItem>,
    ) -> Result<String, EsError> {
        let order_id = Uuid::new_v4().to_string();

        let order_created = OrderCreated {
            order_id: order_id.clone(),
            customer,
            items,
            created_at: OffsetDateTime::now_utc(),
        };

        self.store.append(
            &format!("order-{}", order_id),
            ExpectedVersion::NoStream,
            vec![order_created.into()],
        ).await?;

        Ok(order_id)
    }

    pub async fn add_item_to_order(
        &self,
        order_id: &str,
        item: OrderItem,
        expected_version: i64,
    ) -> Result<i64, EsError> {
        let item_added = OrderItemAdded {
            order_id: order_id.to_string(),
            item,
            added_at: OffsetDateTime::now_utc(),
        };

        let result = self.store.append(
            &format!("order-{}", order_id),
            ExpectedVersion::Exact(expected_version),
            vec![item_added.into()],
        ).await?;

        Ok(result.version)
    }

    pub async fn confirm_order(
        &self,
        order_id: &str,
        expected_version: i64,
    ) -> Result<i64, EsError> {
        let order_confirmed = OrderConfirmed {
            order_id: order_id.to_string(),
            confirmed_at: OffsetDateTime::now_utc(),
        };

        let result = self.store.append(
            &format!("order-{}", order_id),
            ExpectedVersion::Exact(expected_version),
            vec![order_confirmed.into()],
        ).await?;

        Ok(result.version)
    }

    pub async fn process_payment(
        &self,
        order_id: &str,
        payment: PaymentInfo,
        expected_version: i64,
    ) -> Result<i64, EsError> {
        let payment_processed = PaymentProcessed {
            order_id: order_id.to_string(),
            payment,
            processed_at: OffsetDateTime::now_utc(),
        };

        let result = self.store.append(
            &format!("order-{}", order_id),
            ExpectedVersion::Exact(expected_version),
            vec![payment_processed.into()],
        ).await?;

        Ok(result.version)
    }

    pub async fn ship_order(
        &self,
        order_id: &str,
        tracking_number: String,
        expected_version: i64,
    ) -> Result<i64, EsError> {
        let order_shipped = OrderShipped {
            order_id: order_id.to_string(),
            tracking_number,
            shipped_at: OffsetDateTime::now_utc(),
        };

        let result = self.store.append(
            &format!("order-{}", order_id),
            ExpectedVersion::Exact(expected_version),
            vec![order_shipped.into()],
        ).await?;

        Ok(result.version)
    }

    pub async fn cancel_order(
        &self,
        order_id: &str,
        reason: String,
        expected_version: i64,
    ) -> Result<i64, EsError> {
        let order_cancelled = OrderCancelled {
            order_id: order_id.to_string(),
            reason,
            cancelled_at: OffsetDateTime::now_utc(),
        };

        let result = self.store.append(
            &format!("order-{}", order_id),
            ExpectedVersion::Exact(expected_version),
            vec![order_cancelled.into()],
        ).await?;

        Ok(result.version)
    }

    pub async fn get_order_history(&self, order_id: &str) -> Result<Vec<events::EventEnvelope>, EsError> {
        self.store.load(&format!("order-{}", order_id)).await
    }
}
```

## Step 5: Add Business Logic Validation

Let's add some business rules to our service:

```rust
impl OrderService {
    // Add this method to validate order state before operations
    async fn validate_order_state(&self, order_id: &str, allowed_states: &[&str]) -> Result<(), EsError> {
        let events = self.get_order_history(order_id).await?;

        if events.is_empty() {
            return Err(EsError::Cursor("Order not found".into()));
        }

        let current_state = self.determine_current_state(&events);

        if !allowed_states.contains(&current_state.as_str()) {
            return Err(EsError::Cursor(format!(
                "Order is in state '{}', expected one of: {:?}",
                current_state, allowed_states
            )));
        }

        Ok(())
    }

    fn determine_current_state(&self, events: &[events::EventEnvelope]) -> String {
        let mut state = "Created".to_string();

        for event in events {
            match event.r#type.as_str() {
                "OrderConfirmed" => state = "Confirmed".to_string(),
                "PaymentProcessed" => state = "Paid".to_string(),
                "OrderShipped" => state = "Shipped".to_string(),
                "OrderDelivered" => state = "Delivered".to_string(),
                "OrderCancelled" => state = "Cancelled".to_string(),
                _ => {}
            }
        }

        state
    }

    // Update confirm_order to include validation
    pub async fn confirm_order_validated(
        &self,
        order_id: &str,
        expected_version: i64,
    ) -> Result<i64, EsError> {
        // Validate that order can be confirmed
        self.validate_order_state(order_id, &["Created"]).await?;

        self.confirm_order(order_id, expected_version).await
    }
}
```

## Step 6: Create the Main Application

Let's put it all together in a complete application:

```rust
use events::{EventStore, RotationPolicy};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the event store
    let store = Arc::new(
        EventStore::open_partitioned(
            "./order_data",
            RotationPolicy::TimeWindow {
                window: Duration::from_secs(3600), // 1 hour partitions
                max_bytes: Some(512 * 1024 * 1024), // 512MB per partition
            },
        ).await?
    );

    let order_service = OrderService::new(store);

    println!("🛍️  Welcome to the E-commerce Order System!");

    // Create a sample customer
    let customer = CustomerInfo {
        customer_id: "cust-123".to_string(),
        email: "john.doe@example.com".to_string(),
        shipping_address: Address {
            street: "123 Main St".to_string(),
            city: "San Francisco".to_string(),
            state: "CA".to_string(),
            postal_code: "94105".to_string(),
            country: "USA".to_string(),
        },
    };

    // Create order items
    let items = vec![
        OrderItem {
            product_id: "prod-laptop".to_string(),
            quantity: 1,
            unit_price: 129999, // $1,299.99
        },
        OrderItem {
            product_id: "prod-mouse".to_string(),
            quantity: 1,
            unit_price: 2999, // $29.99
        },
    ];

    // Step 1: Create the order
    println!("\n📦 Creating order...");
    let order_id = order_service.create_order(customer.clone(), items.clone()).await?;
    println!("   Order created: {}", order_id);

    // Get initial version
    let history = order_service.get_order_history(&order_id).await?;
    let mut current_version = history.len() as i64;

    // Step 2: Add another item
    println!("\n➕ Adding additional item...");
    let additional_item = OrderItem {
        product_id: "prod-keyboard".to_string(),
        quantity: 1,
        unit_price: 7999, // $79.99
    };

    current_version = order_service.add_item_to_order(
        &order_id,
        additional_item,
        current_version
    ).await?;
    println!("   Item added, new version: {}", current_version);

    // Step 3: Confirm the order
    println!("\n✅ Confirming order...");
    current_version = order_service.confirm_order_validated(&order_id, current_version).await?;
    println!("   Order confirmed, new version: {}", current_version);

    // Step 4: Process payment
    println!("\n💳 Processing payment...");
    let payment = PaymentInfo {
        payment_method_id: "pm-creditcard-123".to_string(),
        amount: 140997, // Total amount in cents
        currency: "USD".to_string(),
    };

    current_version = order_service.process_payment(&order_id, payment, current_version).await?;
    println!("   Payment processed, new version: {}", current_version);

    // Step 5: Ship the order
    println!("\n🚚 Shipping order...");
    let tracking_number = "1Z999AA10123456784".to_string();
    current_version = order_service.ship_order(&order_id, tracking_number, current_version).await?;
    println!("   Order shipped, new version: {}", current_version);

    // Step 6: Display complete order history
    println!("\n📋 Complete Order History:");
    let history = order_service.get_order_history(&order_id).await?;

    for (i, event) in history.iter().enumerate() {
        println!("   {}. {} at {}", i + 1, event.r#type, event.created_at);

        // Display additional context for specific events
        match event.r#type.as_str() {
            "OrderCreated" => {
                if let Ok(order_created) = serde_json::from_value::<OrderCreated>(event.payload.clone()) {
                    println!("      Items: {}", order_created.items.len());
                }
            }
            "OrderShipped" => {
                if let Ok(order_shipped) = serde_json::from_value::<OrderShipped>(event.payload.clone()) {
                    println!("      Tracking: {}", order_shipped.tracking_number);
                }
            }
            _ => {}
        }
    }

    println!("\n🎉 Order processing complete!");
    println!("   Order ID: {}", order_id);
    println!("   Final version: {}", current_version);

    Ok(())
}
```

## Step 7: Add Error Handling

Let's improve our error handling with custom error types:

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum OrderError {
    #[error("Order not found: {0}")]
    OrderNotFound(String),

    #[error("Invalid order state: {0}")]
    InvalidState(String),

    #[error("Insufficient inventory for product: {0}")]
    InsufficientInventory(String),

    #[error("Payment failed: {0}")]
    PaymentFailed(String),

    #[error("Event store error: {0}")]
    EventStoreError(#[from] EsError),
}

impl OrderService {
    pub async fn cancel_order_validated(
        &self,
        order_id: &str,
        reason: String,
        expected_version: i64,
    ) -> Result<i64, OrderError> {
        // Check if order can be cancelled (not shipped or delivered)
        self.validate_order_state(order_id, &["Created", "Confirmed", "Paid"]).await?;

        self.cancel_order(order_id, reason, expected_version)
            .await
            .map_err(OrderError::from)
    }
}
```

## Step 8: Add Unit Tests

Let's add comprehensive tests for our order service:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use events::EventStore;

    async fn setup_test_store() -> Arc<EventStore> {
        let temp_dir = TempDir::new().unwrap();
        let store = EventStore::open_partitioned(
            temp_dir.path(),
            RotationPolicy::TimeWindow {
                window: Duration::from_secs(3600),
                max_bytes: None,
            },
        ).await.unwrap();
        Arc::new(store)
    }

    #[tokio::test]
    async fn test_create_order() {
        let store = setup_test_store().await;
        let service = OrderService::new(store);

        let customer = create_test_customer();
        let items = vec![create_test_item()];

        let order_id = service.create_order(customer, items).await.unwrap();
        assert!(!order_id.is_empty());

        let history = service.get_order_history(&order_id).await.unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].r#type, "OrderCreated");
    }

    #[tokio::test]
    async fn test_confirm_order() {
        let store = setup_test_store().await;
        let service = OrderService::new(store);

        let order_id = create_test_order(&service).await;
        let history = service.get_order_history(&order_id).await.unwrap();
        let initial_version = history.len() as i64;

        let new_version = service.confirm_order_validated(&order_id, initial_version).await.unwrap();
        assert_eq!(new_version, initial_version + 1);

        let updated_history = service.get_order_history(&order_id).await.unwrap();
        assert_eq!(updated_history.len(), 2);
        assert_eq!(updated_history[1].r#type, "OrderConfirmed");
    }

    #[tokio::test]
    async fn test_concurrency_conflict() {
        let store = setup_test_store().await;
        let service = OrderService::new(store);

        let order_id = create_test_order(&service).await;
        let history = service.get_order_history(&order_id).await.unwrap();
        let initial_version = history.len() as i64;

        // Try to confirm with wrong version
        let result = service.confirm_order(&order_id, initial_version + 10).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), EsError::Concurrency { .. }));
    }

    fn create_test_customer() -> CustomerInfo {
        CustomerInfo {
            customer_id: "test-customer".to_string(),
            email: "test@example.com".to_string(),
            shipping_address: Address {
                street: "123 Test St".to_string(),
                city: "Test City".to_string(),
                state: "TS".to_string(),
                postal_code: "12345".to_string(),
                country: "USA".to_string(),
            },
        }
    }

    fn create_test_item() -> OrderItem {
        OrderItem {
            product_id: "test-product".to_string(),
            quantity: 1,
            unit_price: 9999,
        }
    }

    async fn create_test_order(service: &OrderService) -> String {
        service.create_order(create_test_customer(), vec![create_test_item()]).await.unwrap()
    }
}
```

## What We've Built

You've successfully created a complete e-commerce order management system that includes:

✅ **Domain Modeling**: Proper separation of concerns with events and services
✅ **Business Logic**: Validation and state management
✅ **Error Handling**: Comprehensive error types and handling
✅ **Testing**: Unit tests for critical functionality
✅ **Durable Event Streams**: Complete audit trail of all order changes

## Key Concepts Demonstrated

1. **Event-First Design**: All state changes are modeled as events
2. **Immutability**: Events are never changed, only new events are added
3. **Temporal Consistency**: Complete history of every order
4. **Business Logic Separation**: Clean separation between events and business rules
5. **Error Resilience**: Proper handling of concurrency conflicts and validation errors

## Next Steps

Ready to learn more? Try these tutorials:

- [Building Projections](building-projections.md) - Learn how to create read models from your events
- [How to Configure Rotation](../how-to/configure-rotation.md) - Optimize your partitioning strategy
- [How to Handle Concurrency](../how-to/handle-concurrency.md) - Advanced concurrency patterns

## Common Issues and Solutions

**Issue**: "Order already exists" when creating orders
**Solution**: Use UUID generation or a proper ID generation strategy

**Issue**: Performance with many events
**Solution**: Implement snapshots for long-lived aggregates (covered in projections tutorial)

**Issue**: Complex business rules
**Solution**: Use a domain service layer or implement sagas for complex workflows