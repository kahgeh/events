use events::{
    ActorType, EsError, EventNamespaces, EventStreamVersion, ExpectedVersion, NewEvent,
    RotationPolicy, WorkflowRef,
};
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), EsError> {
    tracing_subscriber::fmt::init();

    let namespaces = EventNamespaces::open(
        "./data",
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600),
            max_bytes: Some(1024 * 1024),
        },
    )
    .await?;

    let orders = namespaces.ensure_namespace("orders").await?;
    let partition = orders.ensure_partition("order-123").await?;
    let stream = partition.open().await?;

    let result = stream
        .append(
            ExpectedVersion::NoStream,
            [
                NewEvent {
                    r#type: "OrderCreated".into(),
                    payload: json!({"sku": "ABC", "qty": 1, "price": 29.99}),
                    workflow_kind: Some("fulfilment".into()),
                    workflow: WorkflowRef::StartsThisWorkflow,
                    request_id: None,
                    actor_id: "user-example".to_string(),
                    actor_type: ActorType::User,
                },
                NewEvent {
                    r#type: "PaymentAuthorized".into(),
                    payload: json!({"amount": 2999, "method": "card"}),
                    workflow_kind: None,
                    workflow: WorkflowRef::None,
                    request_id: None,
                    actor_id: "system-payment".to_string(),
                    actor_type: ActorType::System,
                },
            ],
        )
        .await?;

    println!(
        "appended versions {}..{}",
        result.first_version, result.last_version
    );

    let workflow_start = result.events[0]
        .workflow_started_by_event_id
        .expect("workflow starter event has an anchor");

    stream
        .append(
            ExpectedVersion::Exact(result.last_version),
            [NewEvent {
                r#type: "OrderPacked".into(),
                payload: json!({"warehouse": "w1"}),
                workflow_kind: Some("fulfilment".into()),
                workflow: WorkflowRef::Continues {
                    started_by_event_id: workflow_start,
                },
                request_id: None,
                actor_id: "system-warehouse".to_string(),
                actor_type: ActorType::System,
            }],
        )
        .await?;

    let events = stream
        .load_after_version(EventStreamVersion::start(), 100)
        .await?;
    println!("loaded {} event-stream events", events.len());

    let workflow_events = stream
        .load_workflow_after_version(workflow_start, EventStreamVersion::start(), 100)
        .await?;
    println!("loaded {} workflow events", workflow_events.len());

    Ok(())
}
