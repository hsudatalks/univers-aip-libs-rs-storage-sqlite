use univers_aip_lib_storage_sqlite::{
    entity_table_migration_applied, export_entity_tables, import_entity_tables, open_in_memory,
    SqliteQueryAdapter,
};

async fn insert(pool: &sqlx::SqlitePool, table: &str, id: &str, data: &str) {
    sqlx::query(&SqliteQueryAdapter::build_create_table(table))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(&format!(
        "INSERT INTO {table} (id, data, created_at, updated_at, deleted_at) \
         VALUES (?, ?, '2026-01-01', '2026-01-02', NULL)"
    ))
    .bind(id)
    .bind(data)
    .execute(pool)
    .await
    .unwrap();
}

/// Owns the risk that a conflicting ownership transfer partially commits or
/// cannot be retried; an HTTP test cannot observe the SQLite transaction
/// boundary without coupling two live process stores.
#[tokio::test]
async fn snapshot_import_is_atomic_retry_safe_and_conflict_visible() {
    let source = open_in_memory().await.unwrap();
    insert(&source, "workflows", "wf-1", r#"{"name":"source"}"#).await;
    insert(&source, "workflow_versions", "v-1", r#"{"version":1}"#).await;
    let snapshot = export_entity_tables(&source, &["workflow_versions", "workflows"])
        .await
        .unwrap();

    let target = open_in_memory().await.unwrap();
    let report = import_entity_tables(
        &target,
        "workflow_sidecar",
        1,
        &["workflow_versions", "workflows"],
        snapshot.clone(),
    )
    .await
    .unwrap();
    assert_eq!((report.created, report.skipped), (2, 0));
    let retry = import_entity_tables(
        &target,
        "workflow_sidecar",
        1,
        &["workflow_versions", "workflows"],
        snapshot.clone(),
    )
    .await
    .unwrap();
    assert!(!retry.applied);

    let conflicting = open_in_memory().await.unwrap();
    insert(&conflicting, "workflows", "wf-1", r#"{"name":"target"}"#).await;
    let error = import_entity_tables(
        &conflicting,
        "workflow_sidecar",
        1,
        &["workflow_versions", "workflows"],
        snapshot,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("workflows:wf-1"));
    assert!(
        !entity_table_migration_applied(&conflicting, "workflow_sidecar", 1)
            .await
            .unwrap()
    );
    let rolled_back: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master \
         WHERE type = 'table' AND name = 'workflow_versions'",
    )
    .fetch_one(conflicting.as_ref())
    .await
    .unwrap();
    assert_eq!(rolled_back, 0);
}
