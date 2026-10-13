use sqlx::{sqlite::SqliteConnectOptions, Connection};
use std::{path::PathBuf, time::Duration};
use univers_aip_lib_storage_sqlite::{open_with_limits, RepositoryError};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("sqlpool1-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn url(&self) -> String {
        format!("sqlite://{}", self.0.join("store.sqlite").display())
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn seeded_and_subsequent_connections_share_data_and_request_pragmas() {
    let fixture = Fixture::new();
    let timeout = Duration::from_millis(100);
    let pool = open_with_limits(&fixture.url(), 2, timeout).await.unwrap();
    assert_eq!(
        pool.size(),
        1,
        "initial open must return a ready connection"
    );
    let mut first = pool.acquire().await.unwrap();
    sqlx::query("CREATE TABLE proof (value INTEGER)")
        .execute(&mut *first)
        .await
        .unwrap();
    sqlx::query("INSERT INTO proof VALUES (42)")
        .execute(&mut *first)
        .await
        .unwrap();
    let mut second = pool.acquire().await.unwrap();
    let value: i64 = sqlx::query_scalar("SELECT value FROM proof")
        .fetch_one(&mut *second)
        .await
        .unwrap();
    assert_eq!(value, 42, "no private in-memory slot may escape bootstrap");
    for connection in [&mut *first, &mut *second] {
        let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        assert_eq!(mode, "wal");
        for (pragma, expected) in [
            ("foreign_keys", 1_i64),
            ("synchronous", 2),
            ("wal_autocheckpoint", 0),
            ("busy_timeout", 100),
        ] {
            let actual: i64 = sqlx::query_scalar(&format!("PRAGMA {pragma}"))
                .fetch_one(&mut *connection)
                .await
                .unwrap();
            assert_eq!(actual, expected, "{pragma}");
        }
    }
    assert!(
        matches!(pool.acquire().await, Err(sqlx::Error::PoolTimedOut)),
        "request acquisition must remain bounded"
    );
    drop(first);
    drop(second);
    pool.close().await;
    let reopened = open_with_limits(&fixture.url(), 1, timeout).await.unwrap();
    let value: i64 = sqlx::query_scalar("SELECT value FROM proof")
        .fetch_one(reopened.as_ref())
        .await
        .unwrap();
    assert_eq!(value, 42);
    reopened.close().await;
}

async fn lock_rollback_database(fixture: &Fixture) -> sqlx::SqliteConnection {
    let options = fixture
        .url()
        .parse::<SqliteConnectOptions>()
        .unwrap()
        .create_if_missing(true);
    let mut connection = sqlx::SqliteConnection::connect_with(&options)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE proof (value INTEGER)")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut connection)
        .await
        .unwrap();
    connection
}

#[tokio::test]
async fn wal_transition_retries_contention_without_reopening_setup() {
    let fixture = Fixture::new();
    let mut blocker = lock_rollback_database(&fixture).await;
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        sqlx::query("ROLLBACK").execute(&mut blocker).await.unwrap();
        blocker.close().await.unwrap();
    });
    let pool = open_with_limits(&fixture.url(), 1, Duration::from_millis(50))
        .await
        .unwrap();
    release.await.unwrap();
    pool.close().await;
}

#[tokio::test]
async fn persistent_contention_fails_with_phase_and_attempt_count() {
    let fixture = Fixture::new();
    let mut blocker = lock_rollback_database(&fixture).await;
    let error = open_with_limits(&fixture.url(), 1, Duration::from_millis(50))
        .await
        .unwrap_err();
    assert!(
        matches!(&error, RepositoryError::Connection(message) if message.contains("phase WAL setup, attempt 8/8")),
        "{error:?}"
    );
    sqlx::query("ROLLBACK").execute(&mut blocker).await.unwrap();
    blocker.close().await.unwrap();
}

#[tokio::test]
async fn unavailable_storage_and_corruption_keep_typed_phase_diagnostics() {
    let fixture = Fixture::new();
    let absent = format!(
        "sqlite://{}",
        fixture.0.join("missing/store.sqlite").display()
    );
    let error = open_with_limits(&absent, 1, Duration::from_secs(1))
        .await
        .unwrap_err();
    assert!(
        matches!(&error, RepositoryError::Connection(message) if message.contains("phase connection/pragma setup, attempt 1/8")),
        "{error:?}"
    );
    std::fs::write(fixture.0.join("store.sqlite"), b"corrupt storage bytes").unwrap();
    let error = open_with_limits(&fixture.url(), 1, Duration::from_secs(1))
        .await
        .unwrap_err();
    assert!(
        matches!(&error, RepositoryError::Query(message) if message.contains("phase connection/pragma setup, attempt 1/8")),
        "{error:?}"
    );
}

#[tokio::test]
async fn read_only_rollback_store_cannot_silently_skip_wal_setup() {
    let fixture = Fixture::new();
    let mut blocker = lock_rollback_database(&fixture).await;
    sqlx::query("ROLLBACK").execute(&mut blocker).await.unwrap();
    blocker.close().await.unwrap();
    let error = open_with_limits(
        &format!("{}?mode=ro", fixture.url()),
        1,
        Duration::from_secs(1),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(&error, RepositoryError::PermissionDenied(message) if message.contains("phase WAL setup, attempt 1/8")),
        "{error:?}"
    );
}
