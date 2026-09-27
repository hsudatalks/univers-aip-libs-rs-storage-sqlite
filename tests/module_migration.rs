use univers_aip_lib_storage_sqlite::{
    migrate_entity_tables, open_in_memory, sync_entity_tables, SqliteQueryAdapter,
};

#[tokio::test]
async fn sqlite_pools_enforce_foreign_keys() {
    let pool = open_in_memory().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
            .fetch_one(pool.as_ref())
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn module_migration_copies_legacy_rows_once_without_replacing_target_rows() {
    let source = open_in_memory().await.unwrap();
    let target = open_in_memory().await.unwrap();
    sqlx::query(&SqliteQueryAdapter::build_create_table("baselines"))
        .execute(source.as_ref())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO baselines (id, data, created_at, updated_at, deleted_at) \
         VALUES ('legacy', '{\"name\":\"legacy\"}', 'now', 'now', NULL), \
                ('source-only', '{\"name\":\"source\"}', 'now', 'now', NULL)",
    )
    .execute(source.as_ref())
    .await
    .unwrap();
    sqlx::query(&SqliteQueryAdapter::build_create_table("baselines"))
        .execute(target.as_ref())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO baselines (id, data, created_at, updated_at, deleted_at) \
         VALUES ('legacy', '{\"name\":\"target\"}', 'now', 'now', NULL)",
    )
    .execute(target.as_ref())
    .await
    .unwrap();

    let first = migrate_entity_tables(
        Some(source.as_ref()),
        target.as_ref(),
        "mv",
        1,
        &["baselines", "mv_goals"],
    )
    .await
    .unwrap();
    let replay = migrate_entity_tables(
        Some(source.as_ref()),
        target.as_ref(),
        "mv",
        1,
        &["baselines", "mv_goals"],
    )
    .await
    .unwrap();

    assert!(first.applied);
    assert_eq!(first.created, 1);
    assert_eq!(first.skipped, 1);
    assert!(!replay.applied);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM baselines")
            .fetch_one(target.as_ref())
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT data FROM baselines WHERE id = 'legacy'")
            .fetch_one(target.as_ref())
            .await
            .unwrap(),
        "{\"name\":\"target\"}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM module_schema_migrations WHERE module = 'mv' AND version = 1"
        )
        .fetch_one(target.as_ref())
        .await
        .unwrap(),
        1
    );
}

#[tokio::test]
async fn entity_table_sync_replays_after_new_source_rows_without_replacing_target_rows() {
    let source = open_in_memory().await.unwrap();
    let target = open_in_memory().await.unwrap();
    sqlx::query(&SqliteQueryAdapter::build_create_table("tasks"))
        .execute(source.as_ref())
        .await
        .unwrap();
    sqlx::query(&SqliteQueryAdapter::build_create_table("tasks"))
        .execute(target.as_ref())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO tasks (id, data, created_at, updated_at, deleted_at) \
         VALUES ('task-1', '{\"name\":\"source-1\"}', 'now', 'now', NULL)",
    )
    .execute(source.as_ref())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO tasks (id, data, created_at, updated_at, deleted_at) \
         VALUES ('task-1', '{\"name\":\"target-1\"}', 'now', 'now', NULL)",
    )
    .execute(target.as_ref())
    .await
    .unwrap();

    let first = sync_entity_tables(Some(source.as_ref()), target.as_ref(), &["tasks"])
        .await
        .unwrap();
    assert_eq!(first.created, 0);
    assert_eq!(first.skipped, 1);

    sqlx::query(
        "INSERT INTO tasks (id, data, created_at, updated_at, deleted_at) \
         VALUES ('task-2', '{\"name\":\"source-2\"}', 'now', 'now', NULL)",
    )
    .execute(source.as_ref())
    .await
    .unwrap();

    let second = sync_entity_tables(Some(source.as_ref()), target.as_ref(), &["tasks"])
        .await
        .unwrap();
    assert_eq!(second.created, 1);
    assert_eq!(second.skipped, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tasks")
            .fetch_one(target.as_ref())
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT data FROM tasks WHERE id = 'task-1'")
            .fetch_one(target.as_ref())
            .await
            .unwrap(),
        "{\"name\":\"target-1\"}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'module_schema_migrations'"
        )
        .fetch_one(target.as_ref())
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn failed_module_migration_rolls_back_and_can_be_retried() {
    let source = open_in_memory().await.unwrap();
    let target = open_in_memory().await.unwrap();
    sqlx::query("CREATE TABLE baselines (id TEXT PRIMARY KEY, data TEXT NOT NULL)")
        .execute(source.as_ref())
        .await
        .unwrap();
    sqlx::query("INSERT INTO baselines (id, data) VALUES ('legacy', '{}')")
        .execute(source.as_ref())
        .await
        .unwrap();

    migrate_entity_tables(
        Some(source.as_ref()),
        target.as_ref(),
        "mv",
        1,
        &["baselines"],
    )
    .await
    .expect_err("malformed legacy schema should fail");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM module_schema_migrations WHERE module = 'mv' AND version = 1"
        )
        .fetch_one(target.as_ref())
        .await
        .unwrap(),
        0
    );

    sqlx::query("DROP TABLE baselines")
        .execute(source.as_ref())
        .await
        .unwrap();
    sqlx::query(&SqliteQueryAdapter::build_create_table("baselines"))
        .execute(source.as_ref())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO baselines (id, data, created_at, updated_at, deleted_at) \
         VALUES ('legacy', '{}', 'now', 'now', NULL)",
    )
    .execute(source.as_ref())
    .await
    .unwrap();

    let retry = migrate_entity_tables(
        Some(source.as_ref()),
        target.as_ref(),
        "mv",
        1,
        &["baselines"],
    )
    .await
    .unwrap();
    assert!(retry.applied);
    assert_eq!(retry.created, 1);
}
