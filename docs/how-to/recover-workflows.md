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

### 5. Clear Workflow on Success, Preserve on Failure

The key insight is that you should:
- **Clear** the workflow when it completes **successfully**
- **Preserve** the workflow when it **fails** (so recovery can retry)

```rust
async fn process_workflow_event(
    store: &EventStore,
    consumer: &str,
    event: &EventEnvelope,
    cursor: &PartitionedCursor,
) -> Result<Option<ActiveWorkflow>> {
    // Set active workflow BEFORE running the handler
    let active_workflow = ActiveWorkflow {
        stream_id: event.stream_id.clone(),
        event_id: event.id,
    };
    checkpoint(store, consumer, cursor, Some(&active_workflow)).await?;

    // Run the workflow handler
    if let Err(e) = run_workflow_handler(event).await {
        tracing::warn!(error = %e, "Workflow failed, preserving for recovery");
        // Return the workflow so it stays tracked for recovery
        return Ok(Some(active_workflow));
    }

    // Success - clear the workflow
    Ok(None)
}
```

The checkpoint at the end of the batch will then either:
- Clear the workflow (if `None` is returned)
- Preserve the workflow (if `Some(workflow)` is returned)

## Complete Example

Here's a complete example of a projector with workflow recovery, matching the pattern used in alir-platform-services:

```rust
use events::{
    ActiveWorkflow, EventEnvelope, EventStore, PartitionedCursor,
    bootstrap_cursor, checkpoint, get_active_workflow,
};
use std::time::Duration;

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

            // Track active workflow through the batch
            let mut current_workflow: Option<ActiveWorkflow> = None;

            for event in &events {
                match self.process_event(event).await {
                    Ok(Some(workflow)) => {
                        // Workflow failed - preserve it for recovery
                        current_workflow = Some(workflow);
                    }
                    Ok(None) => {
                        // No workflow active (completed or non-workflow event)
                        current_workflow = None;
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to process event");
                        // current_workflow is preserved from before the error
                    }
                }
            }

            // Checkpoint with workflow state
            checkpoint(
                &self.store,
                &self.consumer,
                &next_cursor,
                current_workflow.as_ref(),
            ).await?;

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

        // Re-run the workflow - the handler should be idempotent
        // and skip already-completed steps
        self.run_provisioning_workflow(&events[0]).await?;

        // Clear the workflow after successful recovery
        let cursor = bootstrap_cursor(&self.store, &self.consumer).await?;
        checkpoint(&self.store, &self.consumer, &cursor, None).await?;

        tracing::info!("Workflow recovery completed");
        Ok(())
    }

    /// Process an event and return workflow state.
    ///
    /// Returns:
    /// - `Ok(None)` - No workflow active (completed or non-workflow event)
    /// - `Ok(Some(workflow))` - Workflow failed, preserve for recovery
    async fn process_event(
        &self,
        event: &EventEnvelope,
    ) -> Result<Option<ActiveWorkflow>> {
        match event.r#type.as_str() {
            "PROVISION_REQUESTED" => {
                // Set active workflow BEFORE running handler
                let active_workflow = ActiveWorkflow {
                    stream_id: event.stream_id.clone(),
                    event_id: event.id,
                };
                let cursor = bootstrap_cursor(&self.store, &self.consumer).await?;
                checkpoint(
                    &self.store,
                    &self.consumer,
                    &cursor,
                    Some(&active_workflow),
                ).await?;

                // Run the workflow
                if let Err(e) = self.run_provisioning_workflow(event).await {
                    tracing::warn!(
                        error = %e,
                        "Workflow failed, preserving for recovery"
                    );
                    return Ok(Some(active_workflow));
                }

                // Success - clear workflow
                Ok(None)
            }
            // Checkpoint events - just log, no workflow action
            "MACHINE_CREATED" | "PROVISIONED" | "PROVISION_FAILED" => {
                tracing::debug!(event_type = %event.r#type, "Checkpoint event");
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    async fn run_provisioning_workflow(&self, event: &EventEnvelope) -> Result<()> {
        // Your idempotent workflow logic here
        // Should check what steps are already done and skip them
        todo!()
    }
}
```

### Key Points

1. **Set workflow BEFORE handler runs** - If the process crashes during the handler, the workflow is already recorded for recovery.

2. **Preserve workflow on failure** - Return `Some(workflow)` when the handler fails so it gets checkpointed for recovery.

3. **Clear workflow on success** - Return `None` when the handler succeeds.

4. **Recovery re-runs the workflow** - On startup, if an active workflow is found, re-run it. The handler should be idempotent and skip completed steps.

## Best Practices

### 1. Set Workflow Before Handler, Clear on Success

Set the active workflow **before** running the handler (so crashes during the handler are recoverable). Clear it only when the handler **succeeds**. On failure, preserve it for retry.

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

### 4. Design Idempotent Handlers

The best approach is to make your workflow handlers idempotent - they check what's already done and skip completed steps:

```rust
async fn run_provisioning_workflow(&self, stream_id: &str) -> Result<()> {
    // Check if app already exists
    if !self.has_checkpoint_event(stream_id, "APP_CREATED").await {
        self.create_app().await?;
        self.emit_app_created_event(stream_id).await?;
    }

    // Check if machine already exists
    if !self.has_checkpoint_event(stream_id, "MACHINE_CREATED").await {
        self.create_machine().await?;
        self.emit_machine_created_event(stream_id).await?;
    }

    // All steps complete
    self.emit_provisioned_event(stream_id).await?;
    Ok(())
}
```

This way, recovery simply re-runs the same handler and it skips already-completed steps.

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
