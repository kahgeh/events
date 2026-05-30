use super::*;

#[test]
fn test_valid_table_names() {
    let valid_names = [
        "users",
        "events",
        "user_events",
        "event_stream_2024",
        "app_data",
        "customer_orders",
        "product_catalog",
        "audit_log",
        "session_tokens",
        "user_profiles_v2",
    ];

    for name in &valid_names {
        assert!(
            TableNameValidator::validate_table_name(name).is_ok(),
            "Expected '{}' to be valid",
            name
        );
    }
}

#[test]
fn test_invalid_table_names() {
    let too_long_name = "a".repeat(65);
    let invalid_cases = vec![
        ("", "empty string"),
        ("ab", "too short"),
        (too_long_name.as_str(), "too long"),
        ("123users", "starts with number"),
        ("user-events", "contains dash"),
        ("user events", "contains space"),
        ("user@events", "contains special char"),
        ("user.events", "contains dot"),
        ("user,events", "contains comma"),
        ("user;events", "contains semicolon"),
        ("user'events", "contains quote"),
        ("user\"events", "contains double quote"),
        ("user`events", "contains backtick"),
        ("user(events", "contains parenthesis"),
        ("user[events", "contains bracket"),
        ("user{events", "contains brace"),
        ("user|events", "contains pipe"),
        ("user&events", "contains ampersand"),
        ("user^events", "contains caret"),
        ("user%events", "contains percent"),
        ("user$events", "contains dollar"),
        ("user#events", "contains hash"),
        ("user@events", "contains at"),
        ("user!events", "contains exclamation"),
        ("user~events", "contains tilde"),
        ("user?events", "contains question"),
        ("user\\events", "contains backslash"),
        ("user/events", "contains forward slash"),
        ("user\\nevents", "contains newline sequence"),
        ("user\\x41events", "contains hex escape"),
        ("0x41events", "contains hex prefix"),
        ("SELECT", "SQL keyword"),
        ("select", "SQL keyword lowercase"),
        ("INSERT", "SQL keyword"),
        ("UPDATE", "SQL keyword"),
        ("DELETE", "SQL keyword"),
        ("DROP", "SQL keyword"),
        ("CREATE", "SQL keyword"),
        ("TABLE", "SQL keyword"),
        ("users; DROP TABLE events; --", "SQL injection attempt"),
        ("users'; DROP TABLE events; --", "SQL injection with quote"),
        (
            "users\"; DROP TABLE events; --",
            "SQL injection with double quote",
        ),
        (
            "users`; DROP TABLE events; --",
            "SQL injection with backtick",
        ),
        (
            "users/**/DROP/**/TABLE/**/events",
            "SQL injection with comments",
        ),
        (
            "users UNION SELECT * FROM passwords",
            "SQL injection with UNION",
        ),
        (
            "users'; INSERT INTO users VALUES('hacker', 'password'); --",
            "SQL injection with INSERT",
        ),
        ("__users", "starts with underscore"),
        ("users__", "ends with underscore"),
        ("user__events", "consecutive underscores"),
        ("users ", "trailing space"),
        (" users", "leading space"),
        ("user s", "internal space"),
        ("123", "only numbers"),
        ("中文", "non-ASCII characters"),
        ("café", "non-ASCII characters"),
        ("naïve", "non-ASCII characters"),
    ];

    for (name, description) in invalid_cases {
        assert!(
            TableNameValidator::validate_table_name(name).is_err(),
            "Expected '{}' ({}) to be invalid",
            name,
            description
        );
    }
}

#[test]
fn test_sanitize_table_name() {
    assert!(TableNameValidator::sanitize_table_name("users").is_ok());
    assert!(TableNameValidator::sanitize_table_name("invalid-name").is_err());

    let result = TableNameValidator::sanitize_table_name("valid_name");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "valid_name");
}

#[test]
fn test_pattern_validation() {
    use regex::Regex;

    let pattern = Regex::new(r"^event_[a-zA-Z0-9_]+$").unwrap();

    assert!(TableNameValidator::validate_table_name_with_pattern("event_users", &pattern).is_ok());
    assert!(
        TableNameValidator::validate_table_name_with_pattern("event_stream_2024", &pattern).is_ok()
    );
    assert!(TableNameValidator::validate_table_name_with_pattern("user_events", &pattern).is_err());
    assert!(
        TableNameValidator::validate_table_name_with_pattern("event-invalid", &pattern).is_err()
    );
}

#[test]
fn test_edge_cases() {
    // Test exactly minimum length
    assert!(TableNameValidator::validate_table_name("abc").is_ok());

    // Test exactly maximum length
    let max_len_name = "a".repeat(64);
    assert!(TableNameValidator::validate_table_name(&max_len_name).is_ok());

    // Test one over maximum length
    let too_long_name = "a".repeat(65);
    assert!(TableNameValidator::validate_table_name(&too_long_name).is_err());
}
