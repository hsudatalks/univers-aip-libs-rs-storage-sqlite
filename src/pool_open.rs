//! Bootstrap storage outside SQLx's request acquisition deadline, then seed the
//! request pool with that validated connection. No filesystem operation is
//! cancelled merely because a healthy disk takes longer than the request limit.
use crate::{RepositoryError, RepositoryResult};
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection, SqlitePool, SqlitePoolOptions};
use sqlx::Connection;
use std::{future::Future, time::Duration};

const OPEN_ATTEMPTS: usize = 8;
const STARTUP_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) async fn open_pool(
    options: SqliteConnectOptions,
    pool_options: SqlitePoolOptions,
    request_timeout: Duration,
) -> RepositoryResult<SqlitePool> {
    let startup = options
        .clone()
        .busy_timeout(request_timeout.max(STARTUP_BUSY_TIMEOUT));
    let mut connection = retry("connection/pragma setup", || {
        SqliteConnection::connect_with(&startup)
    })
    .await?;
    // WAL is database-persistent; never repeat its mode transition on request
    // connections or checkpoint connections. Check the result, since SQLite can
    // return the old journal mode without reporting an error.
    let mut current_mode = None;
    for attempt in 1..=OPEN_ATTEMPTS {
        match sqlx::query_scalar::<_, String>("PRAGMA journal_mode")
            .fetch_one(&mut connection)
            .await
        {
            Ok(mode) => {
                current_mode = Some(mode);
                break;
            }
            Err(error) => retry_or_fail("journal mode inspection", attempt, error).await?,
        }
    }
    let mode = current_mode.expect("a failed final attempt returns an error");
    if mode != "wal" && mode != "memory" {
        let mut selected = None;
        for attempt in 1..=OPEN_ATTEMPTS {
            match sqlx::query_scalar::<_, String>("PRAGMA journal_mode = WAL")
                .fetch_one(&mut connection)
                .await
            {
                Ok(mode) => {
                    selected = Some(mode);
                    break;
                }
                Err(error) => retry_or_fail("WAL setup", attempt, error).await?,
            }
        }
        if selected.as_deref() != Some("wal") {
            return Err(RepositoryError::connection(format!(
                "SQLite initial open phase WAL setup: expected wal, got {}",
                selected.unwrap_or_default()
            )));
        }
    }
    // Restore the owner's request busy timeout on the bootstrap connection too.
    let millis = request_timeout.as_millis().min(i32::MAX as u128);
    sqlx::query(&format!("PRAGMA busy_timeout = {millis}"))
        .execute(&mut connection)
        .await
        .map_err(|error| open_error("request busy_timeout", 1, error))?;

    // SQLx exposes no constructor for a pool from an existing connection. Lease
    // one cheap private in-memory slot, replace it through DerefMut, and switch
    // future connections to the real options before publishing the pool. The
    // pool has min_connections=0, so no background connector can race this step.
    let pool = pool_options
        .min_connections(0)
        .connect_lazy_with(SqliteConnectOptions::new().in_memory(true));
    let mut slot = pool
        .acquire()
        .await
        .map_err(|error| open_error("pool seed", 1, error))?;
    std::mem::swap(&mut *slot, &mut connection);
    pool.set_connect_options(options);
    connection
        .close()
        .await
        .map_err(|error| open_error("pool seed cleanup", 1, error))?;
    slot.return_to_pool().await;
    if pool.num_idle() != 1 {
        return Err(RepositoryError::connection(
            "SQLite initial open phase pool seed: validated connection could not be retained",
        ));
    }
    Ok(pool)
}

async fn retry<T, F, Fut>(phase: &str, mut operation: F) -> RepositoryResult<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, sqlx::Error>>,
{
    for attempt in 1..=OPEN_ATTEMPTS {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) => retry_or_fail(phase, attempt, error).await?,
        }
    }
    unreachable!("the final failed attempt returns an error")
}

async fn retry_or_fail(phase: &str, attempt: usize, error: sqlx::Error) -> RepositoryResult<()> {
    if attempt == OPEN_ATTEMPTS || !retryable(&error) {
        return Err(open_error(phase, attempt, error));
    }
    let delay = Duration::from_millis((25 << (attempt - 1)).min(1000));
    tracing::warn!(phase, attempt, %error, ?delay, "SQLite initial open retry");
    tokio::time::sleep(delay).await;
    Ok(())
}

fn primary_code(error: &sqlx::Error) -> Option<i32> {
    error
        .as_database_error()?
        .code()?
        .parse::<i32>()
        .ok()
        .map(|code| code & 0xff)
}

fn retryable(error: &sqlx::Error) -> bool {
    matches!(primary_code(error), Some(5 | 6))
        || matches!(error,
        sqlx::Error::Io(error) if matches!(error.kind(), std::io::ErrorKind::Interrupted | std::io::ErrorKind::WouldBlock))
}

fn open_error(phase: &str, attempt: usize, error: sqlx::Error) -> RepositoryError {
    let message =
        format!("SQLite initial open phase {phase}, attempt {attempt}/{OPEN_ATTEMPTS}: {error}");
    match primary_code(&error) {
        Some(8 | 23) => RepositoryError::permission_denied(message),
        Some(5 | 6 | 10 | 14) => RepositoryError::connection(message),
        Some(_) => RepositoryError::query(message),
        None => match error {
            sqlx::Error::PoolTimedOut => RepositoryError::timeout(message),
            sqlx::Error::Io(_) | sqlx::Error::Configuration(_) => {
                RepositoryError::connection(message)
            }
            _ => RepositoryError::internal(message),
        },
    }
}
