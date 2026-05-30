use crate::{EsError, Result};
use regex::Regex;

/// Comprehensive table name validator to prevent SQL injection attacks
pub struct TableNameValidator;

impl TableNameValidator {
    /// Maximum allowed length for table names
    const MAX_TABLE_NAME_LENGTH: usize = 64;

    /// Minimum allowed length for table names
    const MIN_TABLE_NAME_LENGTH: usize = 3;

    /// SQL keywords that should not be used as table names
    const FORBIDDEN_KEYWORDS: &'static [&'static str] = &[
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "CREATE",
        "ALTER",
        "TRUNCATE",
        "EXEC",
        "EXECUTE",
        "UNION",
        "JOIN",
        "INNER",
        "OUTER",
        "LEFT",
        "RIGHT",
        "FULL",
        "WHERE",
        "HAVING",
        "GROUP",
        "ORDER",
        "BY",
        "LIMIT",
        "OFFSET",
        "DISTINCT",
        "ALL",
        "ANY",
        "EXISTS",
        "IN",
        "LIKE",
        "ILIKE",
        "BETWEEN",
        "IS",
        "NULL",
        "AND",
        "OR",
        "NOT",
        "TRUE",
        "FALSE",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "IF",
        "ELSEIF",
        "WHILE",
        "DO",
        "BEGIN",
        "END",
        "COMMIT",
        "ROLLBACK",
        "TRANSACTION",
        "TABLE",
        "INDEX",
        "VIEW",
        "SEQUENCE",
        "TRIGGER",
        "PROCEDURE",
        "FUNCTION",
        "DATABASE",
        "SCHEMA",
        "USER",
        "ROLE",
        "GRANT",
        "REVOKE",
        "PRIVILEGES",
        "LOCK",
        "UNLOCK",
        "SAVEPOINT",
        "RELEASE",
        "SET",
        "SHOW",
        "DESCRIBE",
        "EXPLAIN",
        "PRAGMA",
        "VACUUM",
        "ANALYZE",
        "REINDEX",
        "ATTACH",
        "DETACH",
        "REPLACE",
        "CONFLICT",
        "ROLLBACK",
        "ABORT",
        "FAIL",
        "IGNORE",
        "RESTRICT",
        "CASCADE",
        "CHECK",
        "DEFAULT",
        "UNIQUE",
        "PRIMARY",
        "FOREIGN",
        "REFERENCES",
        "MATCH",
        "ON",
        "DELETE",
        "UPDATE",
        "DEFERRABLE",
        "INITIALLY",
        "DEFERRED",
        "IMMEDIATE",
        "NOCASE",
        "RTRIM",
        "BLOB",
        "TEXT",
        "INTEGER",
        "REAL",
        "NUMERIC",
        "BOOLEAN",
        "DATETIME",
        "DATE",
        "TIME",
        "TIMESTAMP",
        "VARCHAR",
        "CHAR",
        "DECIMAL",
        "FLOAT",
        "DOUBLE",
        "PRECISION",
        "BIGINT",
        "SMALLINT",
        "TINYINT",
        "BIT",
        "VARBIT",
        "SERIAL",
        "BIGSERIAL",
        "UUID",
        "JSON",
        "JSONB",
        "ARRAY",
        "XML",
        "GEOMETRY",
        "POINT",
        "LINE",
        "POLYGON",
        "CIRCLE",
        "BOX",
        "PATH",
        "INET",
        "CIDR",
        "MACADDR",
        "TEMP",
        "TEMPORARY",
        "UNLOGGED",
        "GLOBAL",
        "LOCAL",
        "MATERIALIZED",
        "RECURSIVE",
        "WINDOW",
        "OVER",
        "PARTITION",
        "ROWS",
        "RANGE",
        "UNBOUNDED",
        "PRECEDING",
        "FOLLOWING",
        "CURRENT",
        "ROW",
        "FIRST",
        "LAST",
        "VALUE",
        "LAG",
        "LEAD",
        "ROW_NUMBER",
        "RANK",
        "DENSE_RANK",
        "PERCENT_RANK",
        "CUME_DIST",
        "NTILE",
        "GENERATED",
        "ALWAYS",
        "STORED",
        "VIRTUAL",
        "IDENTITY",
        "COLLATE",
        "NOCOLLATE",
        "NATURAL",
        "USING",
        "ASC",
        "DESC",
        "NULLS",
        "FIRST",
        "LAST",
        "WITH",
        "RECURSIVE",
        "CYCLE",
        "SEARCH",
        "DEPTH",
        "BREADTH",
        "PRIOR",
        "CONNECT",
        "BY",
        "START",
        "ROOT",
        "SYSDATE",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "LOCALTIME",
        "LOCALTIMESTAMP",
        "SESSION_USER",
        "CURRENT_USER",
        "SYSTEM_USER",
        "USER",
        "VERSION",
        "DATABASE",
        "SCHEMA",
        "LANGUAGE",
        "TRANSACTION",
        "ISOLATION",
        "LEVEL",
        "READ",
        "WRITE",
        "ONLY",
        "COMMITTED",
        "UNCOMMITTED",
        "REPEATABLE",
        "SERIALIZABLE",
        "DEFERRABLE",
        "IMMEDIATE",
        "CONCURRENTLY",
        "IF",
        "EXISTS",
        "NOT",
        "NULL",
        "DISTINCTROW",
        "HIGH",
        "PRIORITY",
        "LOW_PRIORITY",
        "DELAYED",
        "QUICK",
        "IGNORE",
        "FORCE",
        "INDEX",
        "KEY",
        "USE",
        "FORCE",
        "IGNORE",
        "STRAIGHT_JOIN",
        "SQL_SMALL_RESULT",
        "SQL_BIG_RESULT",
        "SQL_BUFFER_RESULT",
        "SQL_CACHE",
        "SQL_NO_CACHE",
        "SQL_CALC_FOUND_ROWS",
        "FOUND_ROWS",
        "LAST_INSERT_ID",
        "ROW_COUNT",
        "GET_LOCK",
        "RELEASE_LOCK",
        "IS_FREE_LOCK",
        "IS_USED_LOCK",
        "MASTER_POS_WAIT",
        "NAME_CONST",
        "SLEEP",
        "BENCHMARK",
        "EXTRACT",
        "POSITION",
        "SUBSTRING",
        "TRIM",
        "UPPER",
        "LOWER",
        "CONCAT",
        "LENGTH",
        "CHAR_LENGTH",
        "OCTET_LENGTH",
        "BIT_LENGTH",
        "COALESCE",
        "NULLIF",
        "IFNULL",
        "CAST",
        "CONVERT",
        "ENCRYPT",
        "DECODE",
        "AES_ENCRYPT",
        "AES_DECRYPT",
        "COMPRESS",
        "UNCOMPRESS",
        "PASSWORD",
        "OLD_PASSWORD",
        "MD5",
        "SHA1",
        "SHA2",
        "RANDOM_BYTES",
        "UUID_TO_BIN",
        "BIN_TO_UUID",
        "IS_UUID",
        "LOAD_FILE",
        "LOAD_DATA",
        "INFILE",
        "REPLACE",
        "INTO",
        "OUTFILE",
        "DUMPFILE",
        "FIELDS",
        "TERMINATED",
        "ENCLOSED",
        "ESCAPED",
        "LINES",
        "STARTING",
        "BY",
        "IGNORE",
        "LINES",
        "ROWS",
        "IDENTIFIED",
        "BY",
        "PASSWORD",
        "SET",
        "PASSWORD",
        "FOR",
        "USER",
        "HOST",
        "GRANT",
        "REVOKE",
        "ALL",
        "PRIVILEGES",
        "OPTION",
        "MAX_QUERIES_PER_HOUR",
        "MAX_UPDATES_PER_HOUR",
        "MAX_CONNECTIONS_PER_HOUR",
        "MAX_USER_CONNECTIONS",
        "WITH",
        "GRANT",
        "REVOKE",
        "OPTION",
        "ADMIN",
        "SUPER",
        "RELOAD",
        "SHUTDOWN",
        "PROCESS",
        "FILE",
        "REPLICATION",
        "CLIENT",
        "SLAVE",
        "REPLICATION_SLAVE",
        "CREATE_USER",
        "CREATE_VIEW",
        "SHOW_VIEW",
        "CREATE_ROUTINE",
        "ALTER_ROUTINE",
        "CREATE_TEMPORARY_TABLES",
        "LOCK_TABLES",
        "EXECUTE",
        "CREATE_TABLESPACE",
        "EVENT",
        "TRIGGER",
        "DELETE_HISTORY",
        "ROLE",
        "APPLICATION_PASSWORD_ADMIN",
        "AUDIT_ADMIN",
        "AUTHENTICATION_POLICY_ADMIN",
        "BACKUP_ADMIN",
        "BINLOG_ADMIN",
        "BINLOG_ENCRYPTION_ADMIN",
        "CLONE_ADMIN",
        "CONNECTION_ADMIN",
        "ENCRYPTION_KEY_ADMIN",
        "FIREWALL_ADMIN",
        "GROUP_REPLICATION_ADMIN",
        "INVOKE_PRIV",
        "PERSIST_RO_VARIABLES_ADMIN",
        "PRIVILEGE_VALIDATION_ADMIN",
        "PROXY",
        "REPLICATION_APPLIER",
        "REPLICATION_SLAVE_ADMIN",
        "RESOURCE_GROUP_ADMIN",
        "RESOURCE_GROUP_USER",
        "ROLE_ADMIN",
        "SESSION_VARIABLES_ADMIN",
        "SET_USER_ID",
        "SYSTEM_VARIABLES_ADMIN",
        "TABLE_ENCRYPTION_ADMIN",
        "VERSION_TOKEN_ADMIN",
        "XA_RECOVER_ADMIN",
        "FLUSH_OPTIMIZER_COSTS",
        "FLUSH_STATUS",
        "FLUSH_TABLES",
        "FLUSH_USER_RESOURCES",
        "FLUSH_HOSTS",
        "FLUSH_LOGS",
        "FLUSH_PRIVILEGES",
        "FLUSH_ENGINE_LOGS",
        "FLUSH_ERROR_LOGS",
        "FLUSH_GENERAL_LOGS",
        "FLUSH_SLOW_LOGS",
        "FLUSH_BINARY_LOGS",
        "FLUSH_RELAY_LOGS",
        "FLUSH_ENGINE_LOGS",
        "FLUSH_ERROR_LOGS",
        "FLUSH_GENERAL_LOGS",
        "FLUSH_SLOW_LOGS",
        "FLUSH_BINARY_LOGS",
        "FLUSH_RELAY_LOGS",
        "RESET_MASTER",
        "RESET_SLAVE",
        "RESET_QUERY_CACHE",
        "RESET_SERVER",
        "FLUSH",
    ];

    /// Validates and sanitizes a table name to prevent SQL injection
    ///
    /// # Security Rules
    /// - Only allows alphanumeric characters and underscores
    /// - Must start with a letter
    /// - Must be between 3-64 characters
    /// - Cannot be a SQL keyword
    /// - Cannot contain dangerous patterns
    /// - Uses only ASCII characters
    ///
    /// # Returns
    /// * `Ok(())` if the table name is valid
    /// * `Err(EsError::InvalidTableName)` with a descriptive error message if invalid
    pub fn validate_table_name(table_name: &str) -> Result<()> {
        // Length validation
        if table_name.is_empty() {
            return Err(EsError::InvalidTableName(
                "Table name cannot be empty".to_string(),
            ));
        }

        if table_name.len() < Self::MIN_TABLE_NAME_LENGTH {
            return Err(EsError::InvalidTableName(format!(
                "Table name must be at least {} characters long",
                Self::MIN_TABLE_NAME_LENGTH
            )));
        }

        if table_name.len() > Self::MAX_TABLE_NAME_LENGTH {
            return Err(EsError::InvalidTableName(format!(
                "Table name cannot exceed {} characters",
                Self::MAX_TABLE_NAME_LENGTH
            )));
        }

        // Character validation - only allow letters, numbers, and underscores
        let valid_chars_regex = Regex::new(r"^[a-zA-Z][a-zA-Z0-9_]*$")
            .map_err(|e| EsError::InvalidTableName(format!("Regex compilation error: {}", e)))?;

        if !valid_chars_regex.is_match(table_name) {
            return Err(EsError::InvalidTableName(
                "Table name must start with a letter and contain only alphanumeric characters and underscores".to_string()
            ));
        }

        // ASCII-only validation
        if !table_name.is_ascii() {
            return Err(EsError::InvalidTableName(
                "Table name must contain only ASCII characters".to_string(),
            ));
        }

        // Forbidden keyword validation (case-insensitive)
        let table_name_upper = table_name.to_uppercase();
        if Self::FORBIDDEN_KEYWORDS.contains(&table_name_upper.as_str()) {
            return Err(EsError::InvalidTableName(format!(
                "Table name '{}' is a reserved SQL keyword and cannot be used",
                table_name
            )));
        }

        // Pattern validation for potential injection attempts
        let dangerous_patterns = [
            "--",
            "/*",
            "*/",
            ";",
            "\"",
            "'",
            "`",
            "=",
            "<",
            ">",
            "(",
            ")",
            "[",
            "]",
            "{",
            "}",
            "|",
            "&",
            "^",
            "%",
            "$",
            "#",
            "@",
            "!",
            "~",
            "?",
            "0x",
            "\\x",
            "UNION",
            "SELECT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "DROP",
            "CREATE",
            "ALTER",
            "EXEC",
            "EXECUTE",
            "SCRIPT",
            "DECLARE",
            "CAST",
            "CONVERT",
            "CHAR",
            "VARCHAR",
            "NVARCHAR",
            "NCHAR",
            "VARBINARY",
            "BINARY",
            "TEXT",
            "NTEXT",
            "IMAGE",
            "CURSOR",
            "PROCEDURE",
            "FUNCTION",
            "TRIGGER",
            "VIEW",
            "INFORMATION_SCHEMA",
            "SYS",
            "MSDB",
            "TEMPDB",
            "MYSQL",
            "PERFORMANCE_SCHEMA",
            "PG_CATALOG",
            "INFORMATION_SCHEMA",
        ];

        let table_name_lower = table_name.to_lowercase();
        for pattern in &dangerous_patterns {
            if table_name_lower.contains(pattern) {
                return Err(EsError::InvalidTableName(format!(
                    "Table name contains potentially dangerous pattern: {}",
                    pattern
                )));
            }
        }

        // Multiple consecutive underscores validation
        if table_name.contains("__") {
            return Err(EsError::InvalidTableName(
                "Table name cannot contain consecutive underscores".to_string(),
            ));
        }

        // Trailing or leading underscore validation
        if table_name.starts_with('_') || table_name.ends_with('_') {
            return Err(EsError::InvalidTableName(
                "Table name cannot start or end with an underscore".to_string(),
            ));
        }

        // Number-only validation (table names must have at least one letter)
        if table_name.chars().all(|c| c.is_numeric()) {
            return Err(EsError::InvalidTableName(
                "Table name must contain at least one letter".to_string(),
            ));
        }

        Ok(())
    }

    /// Validates and returns a sanitized table name
    ///
    /// This is a convenience method that combines validation with returning the validated name
    /// for use in SQL queries (with proper parameterization elsewhere)
    pub fn sanitize_table_name(table_name: &str) -> Result<String> {
        Self::validate_table_name(table_name)?;
        Ok(table_name.to_string())
    }

    /// Checks if a table name follows a specific naming convention pattern
    ///
    /// # Arguments
    /// * `table_name` - The table name to validate
    /// * `pattern` - A regex pattern the table name must match
    ///
    /// # Example
    /// ```
    /// use regex::Regex;
    /// use events::validation::TableNameValidator;
    ///
    /// // Only allow table names starting with "event_"
    /// let pattern = Regex::new(r"^event_[a-zA-Z0-9_]+$").unwrap();
    /// TableNameValidator::validate_table_name_with_pattern("event_users", &pattern).unwrap();
    /// ```
    pub fn validate_table_name_with_pattern(table_name: &str, pattern: &Regex) -> Result<()> {
        Self::validate_table_name(table_name)?;

        if !pattern.is_match(table_name) {
            return Err(EsError::InvalidTableName(format!(
                "Table name '{}' does not match required pattern",
                table_name
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
#[path = "validation_tests.rs"]
mod validation_tests;
