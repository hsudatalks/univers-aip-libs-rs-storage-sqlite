//! Validated SQLite JSON index creation over a caller-selected pool and table.

use univers_aip_contracts_data::storage::{
    validate_entity_identifier, EntityIndexOrder, EntityJsonIndexField, EntityJsonIndexSpec,
    RepositoryResult,
};

use crate::SqlitePool;

/// Ensure a C0-declared JSON query index. The caller selects the database and
/// table; this helper only validates identifiers and renders SQLite DDL.
pub async fn ensure_json_index(
    pool: &SqlitePool,
    table: &str,
    spec: &EntityJsonIndexSpec,
) -> RepositoryResult<()> {
    validate_entity_identifier("entity table", table)?;
    spec.validate()?;
    crate::ensure_index(pool, table, &render_sqlite_json_index(table, spec)).await
}

fn render_sqlite_json_index(table: &str, spec: &EntityJsonIndexSpec) -> String {
    let fields = spec
        .fields
        .iter()
        .map(|field: &EntityJsonIndexField| {
            let path = field.path.join(".");
            let order = match field.order {
                EntityIndexOrder::Ascending => "",
                EntityIndexOrder::Descending => " DESC",
            };
            format!("json_extract(data, '$.{path}'){order}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE INDEX IF NOT EXISTS {} ON {table} ({fields})",
        spec.name
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn validated_json_index_matches_legacy_ddl_and_is_idempotent() {
        let pool = crate::open_in_memory().await.unwrap();
        let spec = EntityJsonIndexSpec::new(
            "idx_receipts_scope_observed",
            vec![
                EntityJsonIndexField::ascending("organization_id"),
                EntityJsonIndexField::descending("observed_at"),
            ],
        );
        assert_eq!(
            render_sqlite_json_index("receipts", &spec),
            "CREATE INDEX IF NOT EXISTS idx_receipts_scope_observed ON receipts (json_extract(data, '$.organization_id'), json_extract(data, '$.observed_at') DESC)"
        );
        ensure_json_index(pool.as_ref(), "receipts", &spec)
            .await
            .unwrap();
        ensure_json_index(pool.as_ref(), "receipts", &spec)
            .await
            .unwrap();
        let ddl: String =
            sqlx::query_scalar("SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?")
                .bind("idx_receipts_scope_observed")
                .fetch_one(pool.as_ref())
                .await
                .unwrap();
        assert!(ddl.contains("json_extract(data, '$.organization_id')"));
        assert!(ddl.contains("json_extract(data, '$.observed_at') DESC"));
    }

    #[tokio::test]
    async fn invalid_table_or_index_is_rejected_before_ddl() {
        let pool = crate::open_in_memory().await.unwrap();
        let spec = EntityJsonIndexSpec::new(
            "idx_valid",
            vec![EntityJsonIndexField::ascending("organization_id")],
        );
        assert!(ensure_json_index(pool.as_ref(), "bad;table", &spec)
            .await
            .is_err());
        let invalid = EntityJsonIndexSpec::new(
            "idx;bad",
            vec![EntityJsonIndexField::ascending("organization_id")],
        );
        assert!(ensure_json_index(pool.as_ref(), "receipts", &invalid)
            .await
            .is_err());
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE name = 'receipts'")
                .fetch_one(pool.as_ref())
                .await
                .unwrap();
        assert_eq!(count, 0);
    }
}
