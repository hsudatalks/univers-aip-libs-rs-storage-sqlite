//! Run explicitly with scripts/sqlpool-pressure.sh; ordinary checks do not inject I/O latency.
use std::time::{Duration, Instant};
use univers_aip_lib_storage_sqlite::{health_check, open, open_maintenance_pool, open_with_limits};

#[tokio::test]
#[ignore = "requires the controlled slow-fsync harness"]
async fn initial_open_waits_for_slow_fsync() {
    let root = std::path::PathBuf::from(std::env::var_os("SQLPOOL1_ROOT").unwrap());
    let runs: usize = std::env::var("SQLPOOL1_RUNS")
        .unwrap_or("30".into())
        .parse()
        .unwrap();
    for run in 0..runs {
        let path = root.join(format!("initial-{}-{run}.sqlite", std::process::id()));
        let url = format!("sqlite://{}", path.display());
        let started = Instant::now();
        let pool = match run % 3 {
            0 => open(&url).await,
            1 => open_with_limits(&url, 2, Duration::from_secs(1)).await,
            _ => open_maintenance_pool(&url, 2, Duration::from_secs(1)).await,
        }
        .expect("healthy slow storage must open");
        assert!(
            started.elapsed() >= Duration::from_secs(6),
            "no fsync delay was injected"
        );
        health_check(pool.as_ref()).await.unwrap();
        sqlx::query("CREATE TABLE proof (value INTEGER)")
            .execute(pool.as_ref())
            .await
            .unwrap();
        sqlx::query("INSERT INTO proof VALUES (42)")
            .execute(pool.as_ref())
            .await
            .unwrap();
        pool.close().await;
        let reopened = open(&url).await.unwrap();
        let value: i64 = sqlx::query_scalar("SELECT value FROM proof")
            .fetch_one(reopened.as_ref())
            .await
            .unwrap();
        assert_eq!(value, 42);
        reopened.close().await;
        eprintln!("SQLPOOL1 run {}/{runs} passed", run + 1);
    }
}
