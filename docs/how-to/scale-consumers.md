# Scale Consumers for High-Volume Event Streams

As your event store grows, you'll need to scale consumers to handle increasing event volume. This guide shows you how to build scalable consumer architectures using the Events crate.

## What You'll Learn

- Horizontal scaling patterns
- Load balancing strategies
- Consumer coordination mechanisms
- Performance optimization techniques
- Handling consumer failures

## Scaling Patterns

### 1. Partition-based Scaling

Different consumers handle different partitions:

```rust
use events::{EventStore, PartitionedCursor};
use std::collections::HashMap;

pub struct PartitionedConsumer {
    store: EventStore,
    assigned_partitions: Vec<String>,
    consumer_id: String,
}

impl PartitionedConsumer {
    pub async fn start(&mut self) -> Result<(), EsError> {
        let mut cursors: HashMap<String, PartitionedCursor> = HashMap::new();

        // Initialize cursors for assigned partitions
        for partition in &self.assigned_partitions {
            let cursor = self.bootstrap_partition_cursor(partition).await?;
            cursors.insert(partition.clone(), cursor);
        }

        // Process events from each partition
        loop {
            for partition in &self.assigned_partitions {
                if let Some(cursor) = cursors.get_mut(partition) {
                    let (events, next_cursor) = self.store
                        .all_since(cursor.clone(), 1000)
                        .await?;

                    if !events.is_empty() {
                        self.process_events(events).await?;
                        *cursor = next_cursor;
                        self.save_checkpoint(partition, cursor).await?;
                    }
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn process_events(&self, events: Vec<EventEnvelope>) -> Result<(), EsError> {
        // Process events from specific partition
        for event in events {
            self.handle_event(event).await?;
        }
        Ok(())
    }
}
```

### 2. Event-type Based Scaling

Different consumers handle different event types:

```rust
pub struct EventTypeConsumer {
    store: EventStore,
    event_types: HashSet<String>,
    consumer_id: String,
}

impl EventTypeConsumer {
    pub async fn start(&mut self) -> Result<(), EsError> {
        let mut cursor = bootstrap_cursor(&self.store, &self.consumer_id).await?;

        loop {
            let (events, next_cursor) = self.store.all_since(cursor, 1000).await?;

            // Filter events by type
            let relevant_events: Vec<_> = events.into_iter()
                .filter(|e| self.event_types.contains(&e.r#type))
                .collect();

            if !relevant_events.is_empty() {
                self.process_events(relevant_events).await?;
            }

            cursor = next_cursor;
            self.save_checkpoint(&cursor).await?;

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
```

### 3. Stream-based Scaling

Different consumers handle different streams:

```rust
pub struct StreamConsumer {
    store: EventStore,
    stream_patterns: Vec<String>,
    consumer_id: String,
}

impl StreamConsumer {
    pub async fn start(&mut self) -> Result<(), EsError> {
        let mut stream_cursors: HashMap<String, PartitionedCursor> = HashMap::new();

        loop {
            // Discover new streams
            let discovered_streams = self.discover_streams().await?;

            // Update cursor map for new streams
            for stream in discovered_streams {
                if !stream_cursors.contains_key(&stream) {
                    let cursor = self.bootstrap_stream_cursor(&stream).await?;
                    stream_cursors.insert(stream, cursor);
                }
            }

            // Process events from assigned streams
            for (stream, cursor) in &mut stream_cursors {
                if self.should_process_stream(stream) {
                    let (events, next_cursor) = self.store
                        .all_since(cursor.clone(), 100)
                        .await?;

                    let stream_events: Vec<_> = events.into_iter()
                        .filter(|e| &e.stream_id == stream)
                        .collect();

                    if !stream_events.is_empty() {
                        self.process_stream_events(stream, stream_events).await?;
                        *cursor = next_cursor;
                        self.save_stream_checkpoint(stream, cursor).await?;
                    }
                }
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn should_process_stream(&self, stream_id: &str) -> bool {
        // Use consistent hashing to assign streams to consumers
        self.stream_patterns.iter().any(|pattern| {
            stream_id.contains(pattern) || self.matches_pattern(stream_id, pattern)
        })
    }

    fn matches_pattern(&self, stream_id: &str, pattern: &str) -> bool {
        // Simple glob matching for stream assignment
        if pattern == "*" {
            return true;
        }

        // More sophisticated matching logic here
        stream_id.starts_with(pattern)
    }
}
```

## Load Balancing Strategies

### Consistent Hashing

```rust
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct ConsistentHashBalancer {
    ring: Vec<u64>,
    nodes: HashMap<u64, String>,
    virtual_nodes: i32,
}

impl ConsistentHashBalancer {
    pub fn new(consumer_ids: Vec<String>, virtual_nodes: i32) -> Self {
        let mut ring = Vec::new();
        let mut nodes = HashMap::new();

        for consumer_id in &consumer_ids {
            for i in 0..virtual_nodes {
                let mut hasher = DefaultHasher::new();
                format!("{}:{}", consumer_id, i).hash(&mut hasher);
                let hash = hasher.finish();

                ring.push(hash);
                nodes.insert(hash, consumer_id.clone());
            }
        }

        ring.sort_unstable();

        Self { ring, nodes, virtual_nodes }
    }

    pub fn get_consumer(&self, key: &str) -> Option<&String> {
        if self.ring.is_empty() {
            return None;
        }

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        // Find first node >= hash
        let pos = self.ring.binary_search(&hash)
            .unwrap_or_else(|pos| pos);

        let node_hash = self.ring[pos % self.ring.len()];
        self.nodes.get(&node_hash)
    }

    pub fn get_partition_assignment(&self, partitions: &[String]) -> HashMap<String, String> {
        let mut assignment = HashMap::new();

        for partition in partitions {
            if let Some(consumer) = self.get_consumer(partition) {
                assignment.insert(partition.clone(), consumer.clone());
            }
        }

        assignment
    }
}
```

### Dynamic Load Balancing

```rust
pub struct DynamicLoadBalancer {
    consumers: HashMap<String, ConsumerStats>,
    assignment_strategy: AssignmentStrategy,
}

pub struct ConsumerStats {
    pub events_processed: AtomicU64,
    pub processing_time_ms: AtomicU64,
    pub error_count: AtomicU64,
    pub last_heartbeat: AtomicU64,
}

pub enum AssignmentStrategy {
    RoundRobin,
    LoadBased,
    LatencyBased,
}

impl DynamicLoadBalancer {
    pub fn new(strategy: AssignmentStrategy) -> Self {
        Self {
            consumers: HashMap::new(),
            assignment_strategy: strategy,
        }
    }

    pub fn register_consumer(&mut self, consumer_id: String) {
        self.consumers.insert(consumer_id, ConsumerStats {
            events_processed: AtomicU64::new(0),
            processing_time_ms: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            last_heartbeat: AtomicU64::new(
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
            ),
        });
    }

    pub fn assign_partition(&self, partition: &str) -> Option<&String> {
        if self.consumers.is_empty() {
            return None;
        }

        match self.assignment_strategy {
            AssignmentStrategy::RoundRobin => {
                // Simple round-robin based on partition hash
                let consumer_ids: Vec<_> = self.consumers.keys().collect();
                let hash = self.partition_hash(partition);
                let index = (hash as usize) % consumer_ids.len();
                consumer_ids.get(index)
            }
            AssignmentStrategy::LoadBased => {
                // Assign to consumer with lowest load
                self.consumers.iter()
                    .min_by_key(|(_, stats)| stats.events_processed.load(Ordering::Relaxed))
                    .map(|(id, _)| id)
            }
            AssignmentStrategy::LatencyBased => {
                // Assign to consumer with lowest average latency
                self.consumers.iter()
                    .filter(|(_, stats)| stats.events_processed.load(Ordering::Relaxed) > 0)
                    .min_by_key(|(_, stats)| {
                        let total_time = stats.processing_time_ms.load(Ordering::Relaxed);
                        let total_events = stats.events_processed.load(Ordering::Relaxed);
                        if total_events > 0 {
                            total_time / total_events
                        } else {
                            u64::MAX
                        }
                    })
                    .map(|(id, _)| id)
            }
        }
    }

    fn partition_hash(&self, partition: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        partition.hash(&mut hasher);
        hasher.finish()
    }
}
```

## Consumer Coordination

### Coordination Service

```rust
pub struct ConsumerCoordinator {
    store: EventStore,
    consumer_group: String,
    consumer_id: String,
    heartbeat_interval: Duration,
    rebalance_interval: Duration,
}

impl ConsumerCoordinator {
    pub async fn start(&mut self) -> Result<(), EsError> {
        let mut heartbeat_timer = interval(self.heartbeat_interval);
        let mut rebalance_timer = interval(self.rebalance_interval);

        // Register this consumer
        self.register_consumer().await?;

        loop {
            tokio::select! {
                _ = heartbeat_timer.tick() => {
                    self.send_heartbeat().await?;
                }
                _ = rebalance_timer.tick() => {
                    self.check_rebalance().await?;
                }
            }
        }
    }

    async fn register_consumer(&self) -> Result<(), EsError> {
        // Use lease mechanism for consumer registration
        let lease_acquired = acquire_lease(
            &self.store,
            &format!("consumer_group:{}:consumer:{}", self.consumer_group, self.consumer_id),
            &self.consumer_id,
            60, // 1 minute lease
        ).await?;

        if !lease_acquired {
            return Err(EsError::Cursor("Failed to register consumer".into()));
        }

        Ok(())
    }

    async fn send_heartbeat(&self) -> Result<(), EsError> {
        renew_lease(
            &self.store,
            &format!("consumer_group:{}:consumer:{}", self.consumer_group, self.consumer_id),
            &self.consumer_id,
        ).await?;

        Ok(())
    }

    async fn check_rebalance(&mut self) -> Result<(), EsError> {
        // Get all active consumers in the group
        let active_consumers = self.get_active_consumers().await?;
        let partitions = self.get_available_partitions().await?;

        // Calculate new assignment
        let assignment = self.calculate_assignment(&active_consumers, &partitions);

        // Update this consumer's assignment if changed
        if let Some(my_assignment) = assignment.get(&self.consumer_id) {
            self.update_assignment(my_assignment).await?;
        }

        Ok(())
    }

    async fn get_active_consumers(&self) -> Result<Vec<String>, EsError> {
        // This would typically query a coordination service
        // For now, we'll use the catalog database
        Ok(vec![]) // Implementation depends on your coordination strategy
    }

    fn calculate_assignment(
        &self,
        consumers: &[String],
        partitions: &[String],
    ) -> HashMap<String, Vec<String>> {
        let balancer = ConsistentHashBalancer::new(consumers.to_vec(), 100);
        let mut assignment: HashMap<String, Vec<String>> = HashMap::new();

        for partition in partitions {
            if let Some(consumer) = balancer.get_consumer(partition) {
                assignment.entry(consumer.clone())
                    .or_insert_with(Vec::new)
                    .push(partition.clone());
            }
        }

        assignment
    }
}
```

## Performance Optimization

### Batch Processing

```rust
pub struct BatchConsumer {
    store: EventStore,
    batch_size: usize,
    batch_timeout: Duration,
    event_buffer: Vec<EventEnvelope>,
    processor: Box<dyn BatchProcessor>,
}

#[async_trait]
pub trait BatchProcessor: Send + Sync {
    async fn process_batch(&self, events: Vec<EventEnvelope>) -> Result<(), EsError>;
}

impl BatchConsumer {
    pub async fn start(&mut self) -> Result<(), EsError> {
        let mut cursor = bootstrap_cursor(&self.store, &self.consumer_id).await?;
        let mut batch_timer = tokio::time::sleep(self.batch_timeout);
        tokio::pin!(batch_timer);

        loop {
            tokio::select! {
                // Get new events
                result = self.store.all_since(cursor.clone(), self.batch_size) => {
                    let (events, next_cursor) = result?;

                    if !events.is_empty() {
                        self.event_buffer.extend(events);
                        cursor = next_cursor;

                        // Process if buffer is full
                        if self.event_buffer.len() >= self.batch_size {
                            self.process_batch().await?;
                            batch_timer.as_mut().reset(tokio::time::Instant::now() + self.batch_timeout);
                        }
                    }
                }

                // Process on timeout
                _ = &mut batch_timer => {
                    if !self.event_buffer.is_empty() {
                        self.process_batch().await?;
                    }
                    batch_timer.as_mut().reset(tokio::time::Instant::now() + self.batch_timeout);
                }
            }
        }
    }

    async fn process_batch(&mut self) -> Result<(), EsError> {
        if self.event_buffer.is_empty() {
            return Ok(());
        }

        let batch = std::mem::take(&mut self.event_buffer);

        let start = std::time::Instant::now();
        self.processor.process_batch(batch).await?;
        let duration = start.elapsed();

        println!("Processed batch of {} events in {:?}", batch.len(), duration);

        Ok(())
    }
}
```

### Parallel Processing

```rust
pub struct ParallelConsumer {
    store: EventStore,
    worker_count: usize,
    processor: Arc<dyn EventProcessor>,
}

#[async_trait]
pub trait EventProcessor: Send + Sync {
    async fn process_event(&self, event: EventEnvelope) -> Result<(), EsError>;
}

impl ParallelConsumer {
    pub async fn start(&self) -> Result<(), EsError> {
        let (event_sender, event_receiver) = tokio::sync::mpsc::channel(10000);
        let processor = Arc::clone(&self.processor);

        // Start worker tasks
        let mut workers = Vec::new();
        for i in 0..self.worker_count {
            let (tx, rx) = tokio::sync::mpsc::channel(1000);
            let receiver = Arc::new(tokio::sync::Mutex::new(rx));
            let processor_clone = Arc::clone(&processor);

            let worker = tokio::spawn(async move {
                loop {
                    let event = {
                        let rx = receiver.lock().await;
                        rx.recv().await
                    };

                    match event {
                        Some(event) => {
                            if let Err(e) = processor_clone.process_event(event).await {
                                eprintln!("Worker {} processing error: {}", i, e);
                            }
                        }
                        None => break,
                    }
                }
            });

            workers.push((worker, tx));
        }

        // Event distribution task
        let distributor = tokio::spawn(async move {
            let mut cursor = bootstrap_cursor(&store, "distributor").await?;
            let mut worker_index = 0;

            loop {
                let (events, next_cursor) = store.all_since(cursor, 1000).await.unwrap();

                for event in events {
                    // Round-robin distribution
                    let (_, worker_tx) = &workers[worker_index];
                    if worker_tx.send(event).await.is_err() {
                        eprintln!("Failed to send event to worker {}", worker_index);
                    }
                    worker_index = (worker_index + 1) % workers.len();
                }

                cursor = next_cursor;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        // Wait for tasks
        distributor.await?;
        for (worker, _) in workers {
            worker.await?;
        }

        Ok(())
    }
}
```

## Error Handling and Recovery

### Dead Letter Queue

```rust
pub struct DeadLetterQueue {
    store: EventStore,
    queue_name: String,
    max_retries: u32,
}

impl DeadLetterQueue {
    pub async fn add_failed_event(
        &self,
        event: EventEnvelope,
        error: String,
        retry_count: u32,
    ) -> Result<(), EsError> {
        if retry_count >= self.max_retries {
            // Add to dead letter queue
            let dlq_event = DeadLetterEvent {
                original_event: event,
                error_message: error,
                retry_count,
                failed_at: Utc::now(),
            };

            self.store.append(
                &format!("dlq:{}", self.queue_name),
                ExpectedVersion::Any,
                vec![NewEvent {
                    r#type: "DeadLetterEvent".into(),
                    payload: serde_json::to_value(dlq_event)?,
                }],
            ).await?;

            println!("Event added to dead letter queue: {}", self.queue_name);
        }

        Ok(())
    }

    pub async fn retry_events(&self) -> Result<(), EsError> {
        let events = self.store.load(&format!("dlq:{}", self.queue_name)).await?;

        for event in events {
            let dlq_event: DeadLetterEvent = serde_json::from_value(event.payload)?;

            // Attempt to reprocess the event
            if let Err(e) = self.reprocess_event(&dlq_event.original_event).await {
                println!("Retry failed for event: {}", e);
            } else {
                println!("Successfully retried event: {}", dlq_event.original_event.id);
            }
        }

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeadLetterEvent {
    pub original_event: EventEnvelope,
    pub error_message: String,
    pub retry_count: u32,
    pub failed_at: DateTime<Utc>,
}
```

## Monitoring and Observability

### Consumer Metrics

```rust
pub struct ConsumerMetrics {
    events_processed: AtomicU64,
    events_failed: AtomicU64,
    processing_time_ms: AtomicU64,
    lag_ms: AtomicU64,
    throughput_events_per_sec: AtomicU64,
}

impl ConsumerMetrics {
    pub fn record_event_processed(&self, processing_time_ms: u64) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
        self.processing_time_ms.fetch_add(processing_time_ms, Ordering::Relaxed);
    }

    pub fn record_event_failed(&self) {
        self.events_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn calculate_throughput(&self, window_seconds: u64) -> f64 {
        let events = self.events_processed.load(Ordering::Relaxed);
        events as f64 / window_seconds as f64
    }

    pub fn get_health_status(&self) -> ConsumerHealth {
        let events_processed = self.events_processed.load(Ordering::Relaxed);
        let events_failed = self.events_failed.load(Ordering::Relaxed);
        let total_time = self.processing_time_ms.load(Ordering::Relaxed);

        let error_rate = if events_processed + events_failed > 0 {
            events_failed as f64 / (events_processed + events_failed) as f64
        } else {
            0.0
        };

        let avg_processing_time = if events_processed > 0 {
            total_time / events_processed
        } else {
            0
        };

        match (error_rate, avg_processing_time) {
            (rate, _) if rate > 0.1 => ConsumerHealth::Unhealthy(format!("High error rate: {:.2}%", rate * 100.0)),
            (_, time) if time > 5000 => ConsumerHealth::Unhealthy(format!("High processing time: {}ms", time)),
            (rate, time) if rate > 0.05 || time > 1000 => ConsumerHealth::Warning(format!("Elevated error rate: {:.2}%, processing time: {}ms", rate * 100.0, time)),
            _ => ConsumerHealth::Healthy,
        }
    }
}

pub enum ConsumerHealth {
    Healthy,
    Warning(String),
    Unhealthy(String),
}
```

## Best Practices

### 1. Consumer Design
- Keep consumers stateless for easy scaling
- Use idempotent processing
- Implement proper error handling and retry logic
- Monitor consumer health and performance

### 2. Load Balancing
- Choose appropriate partitioning strategy
- Monitor consumer load distribution
- Implement dynamic rebalancing
- Handle consumer failures gracefully

### 3. Performance Optimization
- Use appropriate batch sizes
- Implement parallel processing where beneficial
- Optimize database queries
- Monitor and tune garbage collection

### 4. Reliability
- Implement dead letter queues
- Set up proper monitoring and alerting
- Test failure scenarios
- Plan for capacity growth

## Next Steps

- [Handle Concurrency](handle-concurrency.md) - Managing concurrent access
- [Monitor Production](monitor-production.md) - Comprehensive monitoring setup
- [API Reference](../reference/api.md) - Detailed API documentation