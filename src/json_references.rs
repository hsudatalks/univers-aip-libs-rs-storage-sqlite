use sqlx::SqlitePool;
use univers_aip_contracts_data::storage::{RepositoryError, RepositoryResult};

use crate::{is_bare_ident, map_sqlite_error};

/// Add foreign keys backed by generated columns extracted from entity JSON.
///
/// Each declaration is `(child_table, generated_column, json_field, parent_table)`.
/// The JSON field may contain either a `"table:id"` string or a serialized
/// `RecordId` object. Existing orphan references reject the migration.
pub async fn install_json_references(
    pool: &SqlitePool,
    references: &[(&str, &str, &str, &str)],
) -> RepositoryResult<usize> {
    if references.iter().any(|&(table, column, field, parent)| {
        [table, column, field, parent]
            .into_iter()
            .any(|value| !is_bare_ident(value))
    }) {
        return Err(RepositoryError::query(
            "invalid JSON reference migration identifier",
        ));
    }

    let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(pool)
        .await
        .map_err(map_sqlite_error)?;
    if foreign_keys != 1 {
        return Err(RepositoryError::connection(
            "JSON references require PRAGMA foreign_keys=ON",
        ));
    }

    let mut tx = pool.begin().await.map_err(map_sqlite_error)?;
    let mut added = 0;
    for &(table, column, field, parent) in references {
        let exists: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM pragma_table_xinfo('{table}') WHERE name = ?"
        ))
        .bind(column)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlite_error)?;
        if exists != 0 {
            continue;
        }

        let path = format!("$.{field}");
        let object_path = format!("{path}.id");
        let prefix = format!("{parent}:");
        let expression = format!(
            "CASE json_type(data, '{path}') \
             WHEN 'object' THEN json_extract(data, '{object_path}') \
             WHEN 'text' THEN CASE \
               WHEN json_extract(data, '{path}') LIKE '{prefix}%' \
               THEN substr(json_extract(data, '{path}'), {}) \
               ELSE json_extract(data, '{path}') END \
             ELSE NULL END",
            prefix.len() + 1
        );
        sqlx::query(&format!(
            "ALTER TABLE {table} ADD COLUMN {column} TEXT \
             GENERATED ALWAYS AS ({expression}) VIRTUAL \
             REFERENCES {parent}(id) ON DELETE CASCADE"
        ))
        .execute(&mut *tx)
        .await
        .map_err(map_sqlite_error)?;
        added += 1;
    }

    if let Some((table, rowid, parent, fkid)) =
        sqlx::query_as::<_, (String, Option<i64>, String, i64)>(
            "SELECT \"table\", rowid, parent, fkid FROM pragma_foreign_key_check LIMIT 1",
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlite_error)?
    {
        tx.rollback().await.map_err(map_sqlite_error)?;
        return Err(RepositoryError::query(format!(
            "SQLite contains an orphan JSON reference: table={table}, rowid={rowid:?}, parent={parent}, foreign_key={fkid}"
        )));
    }

    tx.commit().await.map_err(map_sqlite_error)?;
    Ok(added)
}
