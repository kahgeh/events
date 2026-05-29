use events::{
    ActorType, EsError, EventEnvelope, EventNamespaces, EventStreamVersion, ExpectedVersion,
    NewEvent, Result, RotationPolicy, WorkflowRef,
};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let root = example_root()?;
    let namespaces = EventNamespaces::open(
        &root,
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600),
            max_bytes: Some(512 * 1024 * 1024),
        },
    )
    .await?;
    let offsets = Arc::new(Mutex::new(HashMap::new()));
    let (dirty_tx, dirty_rx) = mpsc::channel(128);

    let supervisor = tokio::spawn(run_supervisor(
        namespaces.clone(),
        dirty_rx,
        Arc::clone(&offsets),
    ));

    append_order_created(&namespaces, "acme", "order-100").await?;
    dirty_tx
        .send("acme".to_string())
        .await
        .map_err(|_| EsError::Cursor("worker pool stopped".to_string()))?;

    append_order_created(&namespaces, "beta", "order-200").await?;
    dirty_tx
        .send("beta".to_string())
        .await
        .map_err(|_| EsError::Cursor("worker pool stopped".to_string()))?;

    append_order_created(&namespaces, "acme", "order-101").await?;
    dirty_tx
        .send("acme".to_string())
        .await
        .map_err(|_| EsError::Cursor("worker pool stopped".to_string()))?;

    drop(dirty_tx);
    supervisor
        .await
        .map_err(|e| EsError::Cursor(format!("worker pool panicked: {e}")))??;

    println!(
        "partition worker pool drained events under {}",
        root.display()
    );
    Ok(())
}

fn example_root() -> Result<PathBuf> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| EsError::Cursor(format!("clock error: {e}")))?
        .as_millis();
    Ok(std::env::temp_dir().join(format!("events-partition-workers-{timestamp}")))
}

async fn append_order_created(
    namespaces: &EventNamespaces,
    owner_key: &str,
    order_id: &str,
) -> Result<()> {
    let owners = namespaces.ensure_namespace("owners").await?;
    let partition = owners.ensure_partition(owner_key).await?;
    let stream = partition.open().await?;
    let current = stream.current_version().await?;
    let expected = if current.is_start() {
        ExpectedVersion::NoStream
    } else {
        ExpectedVersion::Any
    };

    stream
        .append(
            expected,
            [NewEvent {
                r#type: "OrderCreated".to_string(),
                payload: json!({
                    "owner_key": owner_key,
                    "order_id": order_id,
                }),
                workflow_kind: Some("fulfilment".to_string()),
                workflow: WorkflowRef::StartsThisWorkflow,
                request_id: None,
                actor_id: format!("owner-{owner_key}"),
                actor_type: ActorType::User,
            }],
        )
        .await?;
    Ok(())
}

async fn run_supervisor(
    namespaces: EventNamespaces,
    mut dirty_rx: mpsc::Receiver<String>,
    offsets: Arc<Mutex<HashMap<String, EventStreamVersion>>>,
) -> Result<()> {
    let state = Arc::new(Mutex::new(SupervisorState::default()));
    let mut tasks = JoinSet::<(String, Result<()>)>::new();
    let mut dirty_rx_closed = false;

    loop {
        tokio::select! {
            maybe_owner = dirty_rx.recv(), if !dirty_rx_closed => {
                let Some(owner_key) = maybe_owner else {
                    dirty_rx_closed = true;
                    continue;
                };

                let mut state_guard = state.lock().await;
                if state_guard.active.contains(&owner_key) {
                    state_guard.pending.insert(owner_key);
                    continue;
                }
                state_guard.active.insert(owner_key.clone());
                drop(state_guard);

                spawn_partition_task(&mut tasks, namespaces.clone(), owner_key, Arc::clone(&offsets));
            }
            Some(result) = tasks.join_next() => {
                let (owner_key, task_result) = result
                    .map_err(|e| EsError::Cursor(format!("worker task panicked: {e}")))?;

                let mut state_guard = state.lock().await;
                if task_result.is_err() {
                    state_guard.active.remove(&owner_key);
                    state_guard.pending.remove(&owner_key);
                    drop(state_guard);
                    task_result?;
                    continue;
                }

                if state_guard.pending.remove(&owner_key) {
                    drop(state_guard);
                    spawn_partition_task(
                        &mut tasks,
                        namespaces.clone(),
                        owner_key,
                        Arc::clone(&offsets),
                    );
                } else {
                    state_guard.active.remove(&owner_key);
                }
            }
            else => return Ok(()),
        }
    }
}

#[derive(Default)]
struct SupervisorState {
    active: HashSet<String>,
    pending: HashSet<String>,
}

fn spawn_partition_task(
    tasks: &mut JoinSet<(String, Result<()>)>,
    namespaces: EventNamespaces,
    owner_key: String,
    offsets: Arc<Mutex<HashMap<String, EventStreamVersion>>>,
) {
    tasks.spawn(async move {
        let result = drain_partition(namespaces, owner_key.clone(), offsets).await;
        (owner_key, result)
    });
}

async fn drain_partition(
    namespaces: EventNamespaces,
    owner_key: String,
    offsets: Arc<Mutex<HashMap<String, EventStreamVersion>>>,
) -> Result<()> {
    let owners = namespaces.ensure_namespace("owners").await?;
    let partition = owners.ensure_partition(&owner_key).await?;
    let stream = partition.open().await?;

    loop {
        let cursor = offsets
            .lock()
            .await
            .get(&owner_key)
            .copied()
            .unwrap_or_else(EventStreamVersion::start);
        let events = stream.load_after_version(cursor, 100).await?;

        if events.is_empty() {
            return Ok(());
        }

        for event in &events {
            project_event(&owner_key, event).await?;
        }

        let last_version = events
            .last()
            .expect("events was checked as non-empty")
            .version;
        offsets.lock().await.insert(owner_key.clone(), last_version);
    }
}

async fn project_event(owner_key: &str, event: &EventEnvelope) -> Result<()> {
    println!(
        "projecting owner {} version {} {}: {}",
        owner_key, event.version, event.r#type, event.payload
    );
    Ok(())
}
