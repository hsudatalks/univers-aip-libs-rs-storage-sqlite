use univers_aip_lib_storage_sqlite::{install_json_references, open_in_memory, SqliteQueryAdapter};

const TABLES: &[&str] = &[
    "baselines",
    "verification_models",
    "verification_results",
    "mv_goals",
    "savings_results",
];

const REFERENCES: &[(&str, &str, &str, &str)] = &[
    (
        "verification_models",
        "baseline_ref",
        "baseline_id",
        "baselines",
    ),
    (
        "verification_results",
        "verification_model_ref",
        "verification_model_id",
        "verification_models",
    ),
    ("mv_goals", "baseline_ref", "baseline_id", "baselines"),
    (
        "savings_results",
        "baseline_ref",
        "baseline_id",
        "baselines",
    ),
];

async fn pool() -> sqlx::SqlitePool {
    let pool = open_in_memory().await.unwrap();
    for table in TABLES {
        sqlx::query(&SqliteQueryAdapter::build_create_table(table))
            .execute(pool.as_ref())
            .await
            .unwrap();
    }
    pool.as_ref().clone()
}

async fn insert(pool: &sqlx::SqlitePool, table: &str, id: &str, data: &str) {
    sqlx::query(&format!(
        "INSERT INTO {table} (id, data, created_at, updated_at) VALUES (?, ?, 'now', 'now')"
    ))
    .bind(id)
    .bind(data)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn json_references_enforce_and_cascade() {
    let pool = pool().await;
    insert(&pool, "baselines", "b1", "{}").await;
    insert(&pool, "mv_goals", "g1", r#"{"baseline_id":"baselines:b1"}"#).await;

    assert_eq!(install_json_references(&pool, REFERENCES).await.unwrap(), 4);
    assert_eq!(install_json_references(&pool, REFERENCES).await.unwrap(), 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM pragma_foreign_key_list('mv_goals') \
             WHERE \"table\" = 'baselines' AND \"from\" = 'baseline_ref'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );

    insert(
        &pool,
        "verification_models",
        "v1",
        r#"{"baseline_id":"baselines:b1"}"#,
    )
    .await;
    insert(
        &pool,
        "verification_results",
        "r1",
        r#"{"verification_model_id":"verification_models:v1"}"#,
    )
    .await;
    insert(
        &pool,
        "savings_results",
        "s1",
        r#"{"baseline_id":{"table":"baselines","id":"b1"}}"#,
    )
    .await;
    sqlx::query(
        "INSERT INTO mv_goals (id, data, created_at, updated_at) \
         VALUES ('orphan', '{\"baseline_id\":\"baselines:missing\"}', 'now', 'now')",
    )
    .execute(&pool)
    .await
    .expect_err("orphan goal must be rejected");

    sqlx::query("DELETE FROM baselines WHERE id = 'b1'")
        .execute(&pool)
        .await
        .unwrap();
    for table in TABLES {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(&pool)
                .await
                .unwrap(),
            0,
            "{table} should cascade with its parent"
        );
    }
}

#[tokio::test]
async fn json_references_reject_existing_orphans_atomically() {
    let pool = pool().await;
    insert(
        &pool,
        "mv_goals",
        "orphan",
        r#"{"baseline_id":"baselines:missing"}"#,
    )
    .await;

    let error = install_json_references(&pool, REFERENCES)
        .await
        .expect_err("existing orphan must block migration");
    assert!(error.to_string().contains("orphan JSON reference"));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM pragma_table_xinfo('mv_goals') WHERE name = 'baseline_ref'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
}
