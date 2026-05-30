pub const LAST_PROCESSED_EVENT_SQL: &str = r#"CREATE TABLE IF NOT EXISTS last_processed_event (
    namespace TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    last_processed_event_version INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (namespace, partition_key)
);"#;

pub const WORKFLOW_FAILURES_SQL: &str = r#"CREATE TABLE IF NOT EXISTS workflow_failures (
    namespace TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    workflow_kind TEXT NOT NULL,
    workflow_started_by_event_id TEXT NOT NULL,
    failed_event_version INTEGER NOT NULL,
    error_code TEXT NOT NULL,
    error_message TEXT NOT NULL,
    is_retriable INTEGER NOT NULL,
    failed_at_ms INTEGER NOT NULL,
    failure_count INTEGER NOT NULL DEFAULT 1,
    reset_at_ms INTEGER,
    PRIMARY KEY (namespace, partition_key, workflow_kind, workflow_started_by_event_id)
);
CREATE INDEX IF NOT EXISTS idx_workflow_failures_kind ON workflow_failures(workflow_kind);
CREATE INDEX IF NOT EXISTS idx_workflow_failures_retriable ON workflow_failures(is_retriable);"#;

pub const APP_SCHEMA_SQL: &str = r#"CREATE TABLE IF NOT EXISTS last_processed_event (
    namespace TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    last_processed_event_version INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (namespace, partition_key)
);

CREATE TABLE IF NOT EXISTS workflow_failures (
    namespace TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    workflow_kind TEXT NOT NULL,
    workflow_started_by_event_id TEXT NOT NULL,
    failed_event_version INTEGER NOT NULL,
    error_code TEXT NOT NULL,
    error_message TEXT NOT NULL,
    is_retriable INTEGER NOT NULL,
    failed_at_ms INTEGER NOT NULL,
    failure_count INTEGER NOT NULL DEFAULT 1,
    reset_at_ms INTEGER,
    PRIMARY KEY (namespace, partition_key, workflow_kind, workflow_started_by_event_id)
);
CREATE INDEX IF NOT EXISTS idx_workflow_failures_kind ON workflow_failures(workflow_kind);
CREATE INDEX IF NOT EXISTS idx_workflow_failures_retriable ON workflow_failures(is_retriable);"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_schema_contains_application_tables() {
        assert!(APP_SCHEMA_SQL.contains("CREATE TABLE IF NOT EXISTS last_processed_event"));
        assert!(APP_SCHEMA_SQL.contains("CREATE TABLE IF NOT EXISTS workflow_failures"));
        assert!(APP_SCHEMA_SQL.contains("last_processed_event_version"));
        assert!(APP_SCHEMA_SQL.contains("workflow_started_by_event_id"));
    }

    #[test]
    fn app_schema_excludes_separate_workflow_and_idempotency_tables() {
        let removed_workflow_table = concat!("active_", "workflows");
        let removed_idempotency_table = concat!("applied_", "events");

        assert!(!APP_SCHEMA_SQL.contains(removed_workflow_table));
        assert!(!APP_SCHEMA_SQL.contains(removed_idempotency_table));
    }
}
