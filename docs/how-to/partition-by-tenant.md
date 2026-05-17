# Partition by Tenant or Shard

Use application-level partitioning when different tenants or shards can be
processed independently. Each partition gets its own event store root and its
own projector checkpoint state.

For a complete runnable version of this pattern, see
`examples/tenant_projectors.rs`:

```bash
cargo run --example tenant_projectors
```

This keeps the crate's single-owner model simple: one owner appends to and
checkpoints a store instance, while your application chooses which store handles
each tenant or shard.

## Choose a Partition Key

For a small number of large tenants, use one store per tenant:

```text
./data/tenants/acme/catalog.db
./data/tenants/acme/events_2026_05_16T10.db

./data/tenants/beta/catalog.db
./data/tenants/beta/events_2026_05_16T10.db
```

For many small tenants, hash tenants into a fixed shard count:

```text
client_id -> shard 0..63

./data/shards/00/catalog.db
./data/shards/01/catalog.db
...
./data/shards/63/catalog.db
```

Tenant-isolated stores give stronger operational isolation. Sharded stores keep
the number of open stores and projectors bounded.

## Open a Store for a Tenant

```rust
use events::{EsError, EventStore, Result, RotationPolicy};
use std::path::{Path, PathBuf};
use std::time::Duration;

fn validate_tenant_id(tenant_id: &str) -> Result<&str> {
    let valid = !tenant_id.is_empty()
        && tenant_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_');

    if valid {
        Ok(tenant_id)
    } else {
        Err(EsError::InvalidPath("invalid tenant id".to_string()))
    }
}

async fn open_tenant_store(root: &Path, tenant_id: &str) -> Result<EventStore> {
    let tenant_id = validate_tenant_id(tenant_id)?;
    let path: PathBuf = root.join("tenants").join(tenant_id);

    EventStore::open_partitioned(
        path.to_str()
            .ok_or_else(|| EsError::InvalidPath("tenant path is not UTF-8".to_string()))?,
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600),
            max_bytes: Some(512 * 1024 * 1024),
        },
    )
    .await
}
```

The same consumer name can be reused for every tenant because each tenant has a
separate `consumer_offsets` table:

```text
tenant acme -> consumer "orders-read-model"
tenant beta -> consumer "orders-read-model"
```

## Run Projectors Asynchronously

You do not need one permanent projector task per tenant. Start a projector only
when a tenant has work, let it drain available events, then let it exit after a
short idle grace period.

The application keeps an active set so only one projector per tenant runs at a
time:

```rust
use events::{EventStore, Projector, ProjectorHandler, Result};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use std::time::Duration;

#[derive(Clone)]
struct TenantProjectors {
    root: PathBuf,
    state: Arc<Mutex<SupervisorState>>,
    dirty_tx: mpsc::Sender<String>,
}

impl TenantProjectors {
    async fn notify_dirty(&self, tenant_id: String) -> Result<()> {
        self.dirty_tx
            .send(tenant_id)
            .await
            .map_err(|_| events::EsError::Cursor("projector supervisor stopped".to_string()))
    }
}

async fn run_supervisor<H>(
    root: PathBuf,
    mut dirty_rx: mpsc::Receiver<String>,
    handler: Arc<H>,
    sender: events::StreamEventSender,
) -> Result<()>
where
    H: ProjectorHandler,
{
    let state = Arc::new(Mutex::new(SupervisorState::default()));
    let mut tasks = JoinSet::<(String, Result<()>)>::new();
    let mut dirty_rx_closed = false;

    loop {
        tokio::select! {
            maybe_tenant = dirty_rx.recv(), if !dirty_rx_closed => {
                let Some(tenant_id) = maybe_tenant else {
                    dirty_rx_closed = true;
                    continue;
                };

                let mut state_guard = state.lock().await;
                if state_guard.active.contains(&tenant_id) {
                    state_guard.pending.insert(tenant_id);
                    continue;
                }
                state_guard.active.insert(tenant_id.clone());
                drop(state_guard);

                let root = root.clone();
                let handler = handler.clone();
                let sender = sender.clone();

                tasks.spawn(async move {
                    let result = drain_tenant(root, tenant_id.clone(), handler, sender).await;
                    (tenant_id, result)
                });
            }
            Some(result) = tasks.join_next() => {
                let (tenant_id, task_result) = result
                    .map_err(|e| events::EsError::Cursor(format!("projector task failed: {e}")))?;

                let mut state_guard = state.lock().await;
                if task_result.is_err() {
                    state_guard.active.remove(&tenant_id);
                    state_guard.pending.remove(&tenant_id);
                    drop(state_guard);
                    task_result?;
                    continue;
                }

                if state_guard.pending.remove(&tenant_id) {
                    drop(state_guard);
                    let root = root.clone();
                    let handler = handler.clone();
                    let sender = sender.clone();
                    tasks.spawn(async move {
                        let result = drain_tenant(root, tenant_id.clone(), handler, sender).await;
                        (tenant_id, result)
                    });
                } else {
                    state_guard.active.remove(&tenant_id);
                }
            }
            else => break,
        }
    }

    Ok(())
}

#[derive(Default)]
struct SupervisorState {
    active: HashSet<String>,
    pending: HashSet<String>,
}

async fn drain_tenant<H>(
    root: PathBuf,
    tenant_id: String,
    handler: Arc<H>,
    sender: events::StreamEventSender,
) -> Result<()>
where
    H: ProjectorHandler,
{
    let store = Arc::new(open_tenant_store(&root, &tenant_id).await?);
    let projector = Projector::new(store, "orders-read-model".to_string());

    loop {
        let processed = projector
            .process_next_batch_with_handler(handler.as_ref(), &sender)
            .await?;

        if processed.is_idle() {
            tokio::time::sleep(Duration::from_millis(250)).await;

            if projector
                .process_next_batch_with_handler(handler.as_ref(), &sender)
                .await?
                .is_idle()
            {
                return Ok(());
            }
        }
    }
}
```

The important behavior is the scheduling pattern:

```text
append event for tenant A
  -> send tenant A to dirty queue
  -> supervisor starts tenant A projector if none is active
  -> projector drains batches and checkpoints
  -> projector exits after the tenant is idle
```

`process_next_batch_with_handler` is the crate API this pattern needs: process
one batch, checkpoint progress, and return whether work was found. If you want
an always-on projector instead, use `Projector::run_with_handler`.

## Shard Instead of Tenant

For many small tenants, map tenant IDs to a bounded shard count:

```rust
fn tenant_shard(tenant_id: &str, shard_count: u64) -> u64 {
    let mut hash = 1469598103934665603_u64;
    for byte in tenant_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(1099511628211);
    }
    hash % shard_count
}
```

Then open stores under `./data/shards/{shard}` instead of
`./data/tenants/{tenant_id}`. The projector owner is now the shard, not the
individual tenant.

## What This Does Not Do

This pattern does not provide cluster membership or cross-node ownership. If a
request can arrive on multiple nodes, your application still needs to route the
tenant or shard to the node that owns it. The crate keeps the local event store
and projector checkpointing simple once the request reaches the owner.
