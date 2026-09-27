use std::time::Duration;

use univers_aip_lib_storage_sqlite::open_with_limits;

// Risk: SQLite auto-checkpoint executes WAL transfer and filesystem sync in
// the request that crosses the page threshold. Every pooled connection must
// delegate that work to the owner-managed background checkpoint instead.
#[tokio::test]
async fn file_pool_disables_request_path_auto_checkpoint_on_every_connection() {
    let database = std::env::temp_dir().join(format!(
        "univers-storage-checkpoint-{}-{}.sqlite",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let url = format!("sqlite://{}?mode=rwc", database.display());
    let pool = open_with_limits(&url, 4, Duration::from_secs(1))
        .await
        .unwrap();

    let mut connections = Vec::new();
    for _ in 0..4 {
        connections.push(pool.acquire().await.unwrap());
    }
    for mut connection in connections {
        let auto_checkpoint: i64 = sqlx::query_scalar("PRAGMA wal_autocheckpoint")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        assert_eq!(auto_checkpoint, 0);
    }

    pool.close().await;
    for path in [
        database.clone(),
        database.with_extension("sqlite-shm"),
        database.with_extension("sqlite-wal"),
    ] {
        let _ = std::fs::remove_file(path);
    }
}
