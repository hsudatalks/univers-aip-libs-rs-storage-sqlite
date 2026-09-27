//! Error Mapping for SQLite
//!
//! Maps `sqlx::Error` to the database-agnostic `RepositoryError`.
//!
//! # SQLite Result Codes
//!
//! SQLite reports extended result codes as numeric strings via
//! `db.code()`. The codes handled here:
//! - `2067` / `1555`: UNIQUE / PRIMARY KEY constraint (duplicate)
//! - `787`: FOREIGN KEY constraint
//! - `275` / `2575`: CHECK constraint
//! - `1299` / `2579`: NOT NULL constraint
//! - `5` / `6`: database busy / locked (transient, retryable by caller)
//! - `14`: cannot open database file
//!
//! Note: with `INSERT ... ON CONFLICT DO NOTHING`, duplicate inserts return
//! `Ok(None)` rather than an error, so the `Duplicate` path is only hit by
//! callers that bypass the idempotent insert.

use univers_aip_contracts_data::storage::RepositoryError;

/// Map an `sqlx::Error` to a `RepositoryError`.
pub fn map_sqlite_error(err: sqlx::Error) -> RepositoryError {
    match &err {
        sqlx::Error::RowNotFound => RepositoryError::not_found("row not found"),

        sqlx::Error::Database(db_err) => {
            let code = db_err.code().map(|c| c.to_string()).unwrap_or_default();
            let message = db_err.message();

            match code.as_str() {
                // UNIQUE / PRIMARY KEY violation
                "2067" | "1555" => {
                    RepositoryError::duplicate(format!("Unique constraint violated: {message}"))
                }
                // FOREIGN KEY violation
                "787" => RepositoryError::query(format!("Foreign key violated: {message}")),
                // CHECK constraint
                "275" | "2575" => {
                    RepositoryError::validation(format!("Check constraint violated: {message}"))
                }
                // NOT NULL violation
                "1299" | "2579" => {
                    RepositoryError::validation(format!("Not-null violated: {message}"))
                }
                // Database busy / locked — transient
                "5" | "6" => {
                    RepositoryError::connection(format!("Database busy/locked: {message}"))
                }
                // Cannot open database file
                "14" => RepositoryError::connection(format!("Cannot open database: {message}")),
                // Read-only / permission
                "8" | "23" => RepositoryError::permission_denied(message.to_string()),
                // Default: generic query error with code
                _ => RepositoryError::query(format!("SQLite error [{code}]: {message}")),
            }
        }

        sqlx::Error::PoolTimedOut => {
            RepositoryError::timeout("Connection pool timeout: no available connections")
        }
        sqlx::Error::PoolClosed => RepositoryError::connection("Connection pool is closed"),

        sqlx::Error::Io(io_err) => RepositoryError::connection(format!("IO error: {io_err}")),

        sqlx::Error::ColumnNotFound(col) => {
            RepositoryError::query(format!("Column not found: {col}"))
        }

        sqlx::Error::ColumnDecode { index, source } => {
            RepositoryError::serialization(format!("Failed to decode column {index}: {source}"))
        }

        sqlx::Error::Decode(decode_err) => {
            RepositoryError::serialization(format!("Decode error: {decode_err}"))
        }

        sqlx::Error::Configuration(config_err) => {
            RepositoryError::connection(format!("Configuration error: {config_err}"))
        }

        sqlx::Error::WorkerCrashed => RepositoryError::internal("Database worker crashed"),

        // Catch-all
        _ => RepositoryError::internal(err.to_string()),
    }
}

/// Whether an error is transient enough to retry (busy/locked, pool timeout, IO).
#[allow(dead_code)]
pub fn is_retryable(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::PoolTimedOut => true,
        sqlx::Error::Io(_) => true,
        sqlx::Error::Database(db_err) => {
            let code = db_err.code().map(|c| c.to_string()).unwrap_or_default();
            matches!(code.as_str(), "5" | "6")
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_not_found_maps_to_not_found() {
        let err = map_sqlite_error(sqlx::Error::RowNotFound);
        assert!(err.is_not_found());
    }

    #[test]
    fn pool_timeout_maps_to_timeout() {
        let err = map_sqlite_error(sqlx::Error::PoolTimedOut);
        assert!(matches!(err, RepositoryError::Timeout(_)));
    }
}
