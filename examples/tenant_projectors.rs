use events::{
    create_broadcast_system, ActorType, EsError, EventEnvelope, EventStore, ExpectedVersion,
    NewEvent, Projector, ProjectorBatchOutcome, ProjectorHandler, ProjectorHandlerError, Result,
    RotationPolicy, StreamEventSender,
};
use serde_json::json;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let root = example_root()?;
    let (dirty_tx, dirty_rx) = mpsc::channel(128);
    let (stream_event_sender, _subscriber, broadcast_loop) = create_broadcast_system();
    tokio::spawn(broadcast_loop.run());

    let handler = Arc::new(PrintOrderHandler);
    let supervisor = tokio::spawn(run_supervisor(
        root.clone(),
        dirty_rx,
        handler,
        stream_event_sender,
    ));

    append_order_created(&root, "acme", "order-100").await?;
    dirty_tx
        .send("acme".to_string())
        .await
        .map_err(|_| EsError::Cursor("projector supervisor stopped".to_string()))?;

    append_order_created(&root, "beta", "order-200").await?;
    dirty_tx
        .send("beta".to_string())
        .await
        .map_err(|_| EsError::Cursor("projector supervisor stopped".to_string()))?;

    append_order_created(&root, "acme", "order-101").await?;
    dirty_tx
        .send("acme".to_string())
        .await
        .map_err(|_| EsError::Cursor("projector supervisor stopped".to_string()))?;

    drop(dirty_tx);

    supervisor
        .await
        .map_err(|e| EsError::Cursor(format!("projector supervisor panicked: {e}")))??;

    println!("tenant projectors drained events under {}", root.display());
    Ok(())
}

fn example_root() -> Result<PathBuf> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| EsError::Cursor(format!("clock error: {e}")))?
        .as_millis();
    Ok(std::env::temp_dir().join(format!("events-tenant-projectors-{timestamp}")))
}

async fn append_order_created(root: &Path, tenant_id: &str, order_id: &str) -> Result<()> {
    let store = open_tenant_store(root, tenant_id).await?;
    store
        .append(
            order_id,
            ExpectedVersion::NoStream,
            vec![NewEvent {
                r#type: "OrderCreated".to_string(),
                payload: json!({
                    "tenant_id": tenant_id,
                    "order_id": order_id,
                }),
                request_id: None,
                actor_id: format!("tenant:{tenant_id}"),
                actor_type: ActorType::User,
            }],
        )
        .await?;
    Ok(())
}

async fn run_supervisor<H>(
    root: PathBuf,
    mut dirty_rx: mpsc::Receiver<String>,
    handler: Arc<H>,
    stream_event_sender: StreamEventSender,
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

                spawn_tenant_task(&mut tasks, root.clone(), tenant_id, handler.clone(), stream_event_sender.clone());
            }
            Some(result) = tasks.join_next() => {
                let (tenant_id, task_result) = result
                    .map_err(|e| EsError::Cursor(format!("projector task panicked: {e}")))?;

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
                    spawn_tenant_task(
                        &mut tasks,
                        root.clone(),
                        tenant_id,
                        handler.clone(),
                        stream_event_sender.clone(),
                    );
                } else {
                    state_guard.active.remove(&tenant_id);
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

fn spawn_tenant_task<H>(
    tasks: &mut JoinSet<(String, Result<()>)>,
    root: PathBuf,
    tenant_id: String,
    handler: Arc<H>,
    stream_event_sender: StreamEventSender,
) where
    H: ProjectorHandler,
{
    tasks.spawn(async move {
        let result = drain_tenant(root, tenant_id.clone(), handler, stream_event_sender).await;
        (tenant_id, result)
    });
}

async fn drain_tenant<H>(
    root: PathBuf,
    tenant_id: String,
    handler: Arc<H>,
    stream_event_sender: StreamEventSender,
) -> Result<()>
where
    H: ProjectorHandler,
{
    let store = Arc::new(open_tenant_store(&root, &tenant_id).await?);
    let projector = Projector::new(store, "orders-read-model".to_string()).with_batch_size(100);

    loop {
        match projector
            .process_next_batch_with_handler(handler.as_ref(), &stream_event_sender)
            .await?
        {
            ProjectorBatchOutcome::Processed { count } => {
                println!("tenant {tenant_id}: processed {count} events");
            }
            ProjectorBatchOutcome::Failed { error, .. } => {
                eprintln!("tenant {tenant_id}: handler failed, retrying: {error}");
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            ProjectorBatchOutcome::Idle => {
                tokio::time::sleep(Duration::from_millis(250)).await;
                let grace_outcome = projector
                    .process_next_batch_with_handler(handler.as_ref(), &stream_event_sender)
                    .await?;

                match grace_outcome {
                    ProjectorBatchOutcome::Idle => {
                        println!("tenant {tenant_id}: idle, stopping projector");
                        return Ok(());
                    }
                    ProjectorBatchOutcome::Processed { count } => {
                        println!("tenant {tenant_id}: processed {count} events");
                    }
                    ProjectorBatchOutcome::Failed { error, .. } => {
                        eprintln!("tenant {tenant_id}: handler failed, retrying: {error}");
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                }
            }
        }
    }
}

async fn open_tenant_store(root: &Path, tenant_id: &str) -> Result<EventStore> {
    let tenant_id = validate_tenant_id(tenant_id)?;
    let path = root.join("tenants").join(tenant_id);
    let path = path
        .to_str()
        .ok_or_else(|| EsError::InvalidPath("tenant path is not UTF-8".to_string()))?;

    EventStore::open_partitioned(
        path,
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600),
            max_bytes: Some(512 * 1024 * 1024),
        },
    )
    .await
}

fn validate_tenant_id(tenant_id: &str) -> Result<&str> {
    let valid = !tenant_id.is_empty()
        && tenant_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_');

    if valid {
        return Ok(tenant_id);
    }

    Err(EsError::InvalidPath("invalid tenant id".to_string()))
}

struct PrintOrderHandler;

impl ProjectorHandler for PrintOrderHandler {
    async fn handle_event(
        &self,
        event: &EventEnvelope,
        _stream_event_sender: &StreamEventSender,
    ) -> std::result::Result<(), ProjectorHandlerError> {
        println!(
            "projecting {} from stream {}: {}",
            event.r#type, event.stream_id, event.payload
        );
        Ok(())
    }
}
