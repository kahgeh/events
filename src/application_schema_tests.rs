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
