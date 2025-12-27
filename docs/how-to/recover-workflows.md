# How to Recover Workflows After Crash

This guide explains how to use the workflow tracking feature to recover incomplete workflows when a projector restarts after a crash.

## Overview

When processing multi-step workflows (like provisioning a user's application), a crash mid-workflow can leave the system in an inconsistent state. The workflow tracking feature allows projectors to:

1. Track which workflow is currently active during checkpoints
2. Detect incomplete workflows on startup
3. Load workflow events to derive current state
4. Resume or clean up the incomplete workflow

## When to Use Workflow Tracking

Use workflow tracking when:

- **Multi-step operations**: The workflow spans multiple events (e.g., PROVISION_REQUESTED → MACHINE_CREATED → VOLUME_ATTACHED → PROVISIONED)
- **External side effects**: The workflow makes changes to external systems (e.g., Fly.io machines)
- **Recovery is complex**: Simply replaying events isn't sufficient; you need to understand where the workflow stopped

## Implementation Pattern

### 1. Define Your Workflow Events

First, identify the events that make up your workflow:

```rust
// Example: Provisioning workflow events
const PROVISION_REQUESTED: &str = "PROVISION_REQUESTED";
const MACHINE_CREATED: &str = "MACHINE_CREATED";
const VOLUME_ATTACHED: &str = "VOLUME_ATTACHED";
const PROVISIONED: &str = "PROVISIONED";
const PROVISION_FAILED: &str = "PROVISION_FAILED";
```

### 2. Check for Active Workflow on Startup

Before starting normal event processing, check if there's an incomplete workflow:

```rust
use events::{EventStore, get_active_workflow, load_since_event};

async fn start_projector(store: &EventStore, consumer: &str) -> Result<()> {
    // Check for active workflow on startup
    if let Some(workflow) = get_active_workflow(store, consumer).await? {
        tracing::info!(
            stream_id = %workflow.stream_id,
            event_id = %workflow.event_id,
            "Found incomplete workflow, recovering..."
        );

        // Load events since workflow start
        let events = store.load_since_event(
            &workflow.stream_id,
            workflow.event_id
        ).await?;

        // Handle the incomplete workflow
        recover_workflow(&events).await?;
    }

    // Continue with normal event processing loop
    run_event_loop(store, consumer).await
}
```

### 3. Derive Workflow State from Events

Analyze the events to understand where the workflow stopped:

```rust
async fn recover_workflow(events: &[EventEnvelope]) -> Result<()> {
    // Derive current state from events
    let mut machine_created = false;
    let mut volume_attached = false;
    let mut completed = false;

    for event in events {
        match event.r#type.as_str() {
            "MACHINE_CREATED" => machine_created = true,
            "VOLUME_ATTACHED" => volume_attached = true,
            "PROVISIONED" | "PROVISION_FAILED" => completed = true,
            _ => {}
        }
    }

    // Decide recovery action based on state
    if completed {
        // Workflow already completed, nothing to do
        tracing::info!("Workflow was already completed");
        return Ok(());
    }

    if machine_created && !volume_attached {
        // Crashed after creating machine but before attaching volume
        // Option 1: Resume from where we left off
        // Option 2: Clean up the partial resources
        resume_or_cleanup_workflow().await?;
    }

    Ok(())
}
```

### 4. Track Active Workflow During Processing

When processing workflow events, track the active workflow in checkpoints:

```rust
use events::{ActiveWorkflow, checkpoint, PartitionedCursor};

async fn handle_provision_requested(
    store: &EventStore,
    consumer: &str,
    event: &EventEnvelope,
    cursor: &PartitionedCursor,
) -> Result<()> {
    // This is the start of a new workflow - track it
    let workflow = ActiveWorkflow {
        stream_id: event.stream_id.clone(),
        event_id: event.id,
    };

    // Checkpoint with the active workflow
    checkpoint(store, consumer, cursor, Some(&workflow)).await?;

    // Now process the event...
    create_machine(&event.payload).await?;

    Ok(())
}
```

### 5. Clear Workflow on Completion

When the workflow completes (success or failure), clear the active workflow:

```rust
async fn handle_workflow_terminal_event(
    store: &EventStore,
    consumer: &str,
    event: &EventEnvelope,
    cursor: &PartitionedCursor,
) -> Result<()> {
    match event.r#type.as_str() {
        "PROVISIONED" | "PROVISION_FAILED" => {
            // Workflow is complete - clear the active workflow
            checkpoint(store, consumer, cursor, None).await?;
        }
        _ => {
            // Mid-workflow event - keep tracking the workflow
            // (workflow was set when PROVISION_REQUESTED was processed)
        }
    }

    Ok(())
}
```

## Complete Example

Here's a complete example of a projector with workflow recovery:

```rust
use events::{
    ActiveWorkflow, EventEnvelope, EventStore,
    bootstrap_cursor, checkpoint, get_active_workflow,
};

struct ProvisioningProjector {
    store: EventStore,
    consumer: String,
}

impl ProvisioningProjector {
    pub async fn run(&self) -> Result<()> {
        // Step 1: Check for incomplete workflow on startup
        self.recover_if_needed().await?;

        // Step 2: Bootstrap cursor
        let mut cursor = bootstrap_cursor(&self.store, &self.consumer).await?;

        // Step 3: Event processing loop
        loop {
            let (events, next_cursor) = self.store
                .all_since(cursor.clone(), 100)
                .await?;

            if events.is_empty() {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            for event in &events {
                self.handle_event(event, &next_cursor).await?;
            }

            cursor = next_cursor;
        }
    }

    async fn recover_if_needed(&self) -> Result<()> {
        let Some(workflow) = get_active_workflow(&self.store, &self.consumer).await?
        else {
            return Ok(());
        };

        tracing::warn!(
            stream_id = %workflow.stream_id,
            event_id = %workflow.event_id,
            "Recovering incomplete workflow"
        );

        let events = self.store
            .load_since_event(&workflow.stream_id, workflow.event_id)
            .await?;

        // Analyze events and take recovery action
        self.recover_workflow(&workflow, &events).await
    }

    async fn recover_workflow(
        &self,
        workflow: &ActiveWorkflow,
        events: &[EventEnvelope],
    ) -> Result<()> {
        // Determine what state the workflow was in
        let last_event = events.last();

        match last_event.map(|e| e.r#type.as_str()) {
            Some("PROVISIONED") | Some("PROVISION_FAILED") => {
                // Already completed, just clear the workflow tracking
                tracing::info!("Workflow was already completed");
            }
            Some("MACHINE_CREATED") => {
                // Crashed after machine creation
                tracing::warn!("Resuming from MACHINE_CREATED state");
                // Resume: attach volume, configure, etc.
            }
            Some("PROVISION_REQUESTED") => {
                // Crashed before any progress
                tracing::warn!("Restarting workflow from beginning");
                // Start fresh
            }
            _ => {
                tracing::error!("Unknown workflow state, manual intervention needed");
            }
        }

        Ok(())
    }

    async fn handle_event(
        &self,
        event: &EventEnvelope,
        cursor: &PartitionedCursor,
    ) -> Result<()> {
        match event.r#type.as_str() {
            "PROVISION_REQUESTED" => {
                // Start tracking this workflow
                let workflow = ActiveWorkflow {
                    stream_id: event.stream_id.clone(),
                    event_id: event.id,
                };
                checkpoint(&self.store, &self.consumer, cursor, Some(&workflow)).await?;

                // Process...
            }
            "PROVISIONED" | "PROVISION_FAILED" => {
                // Clear workflow tracking
                checkpoint(&self.store, &self.consumer, cursor, None).await?;
            }
            _ => {
                // Regular checkpoint (keeps existing workflow if any)
                // Note: mid-workflow events don't need to update workflow tracking
            }
        }

        Ok(())
    }
}
```

## Best Practices

### 1. Track Workflow at Start, Clear at End

Only set the active workflow when processing the workflow's start event. Only clear it when processing a terminal event (success or failure).

### 2. Make Recovery Idempotent

Recovery actions should be safe to run multiple times. Use idempotent operations or check for existing resources before creating new ones.

### 3. Log Recovery Actions

Always log when recovering a workflow so you can audit what happened:

```rust
tracing::warn!(
    stream_id = %workflow.stream_id,
    event_id = %workflow.event_id,
    last_event_type = %last_event_type,
    "Recovering incomplete workflow"
);
```

### 4. Consider Cleanup vs Resume

For some workflows, it may be safer to clean up partial resources and fail the operation rather than trying to resume:

```rust
// Sometimes cleanup is safer than resume
if partial_state_is_inconsistent(&events) {
    cleanup_partial_resources(&events).await?;
    emit_failure_event(stream_id, "Cleaned up after crash").await?;
} else {
    resume_workflow(&events).await?;
}
```

### 5. Handle Recovery Failures

If recovery itself fails, log the error and potentially alert operators:

```rust
if let Err(e) = recover_workflow(&events).await {
    tracing::error!(
        error = %e,
        stream_id = %workflow.stream_id,
        "Workflow recovery failed, manual intervention required"
    );
    // Optionally: send alert, set status flag, etc.
}
```

## Related Documentation

- [Architecture: Workflow Recovery](../explanation/architecture.md#workflow-recovery) - Design rationale
- [API Reference: ActiveWorkflow](../reference/api.md#activeworkflow) - Type documentation
- [API Reference: get_active_workflow](../reference/api.md#get_active_workflow) - Function documentation
- [API Reference: load_since_event](../reference/api.md#load_since_event) - Function documentation
