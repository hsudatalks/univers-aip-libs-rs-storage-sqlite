use super::{
    is_bare_ident, map_sqlite_error, EntityStorageSnapshot, EntityTableSnapshot,
    EntityTableSnapshotRow, EntityTableSyncReport, ModuleMigrationReport, RepoError,
    RepositoryResult, Row, SqlitePool, SqliteQueryAdapter,
};

/// Export selected entity tables, including empty or not-yet-created tables.
pub async fn export_entity_tables(
    source: &SqlitePool,
    tables: &[&str],
) -> RepositoryResult<EntityStorageSnapshot> {
    if tables.iter().any(|table| !is_bare_ident(table)) {
        return Err(RepoError::query("invalid entity snapshot table name"));
    }
    let mut snapshot = EntityStorageSnapshot::default();
    for table in tables {
        let exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(*table)
        .fetch_one(source)
        .await
        .map_err(map_sqlite_error)?;
        let rows = if exists == 0 {
            Vec::new()
        } else {
            sqlx::query(&format!(
                "SELECT id, data, created_at, updated_at, deleted_at FROM {table} ORDER BY id"
            ))
            .fetch_all(source)
            .await
            .map_err(map_sqlite_error)?
            .into_iter()
            .map(|row| {
                Ok(EntityTableSnapshotRow {
                    id: row.try_get("id").map_err(map_sqlite_error)?,
                    data: row.try_get("data").map_err(map_sqlite_error)?,
                    created_at: row.try_get("created_at").map_err(map_sqlite_error)?,
                    updated_at: row.try_get("updated_at").map_err(map_sqlite_error)?,
                    deleted_at: row.try_get("deleted_at").map_err(map_sqlite_error)?,
                })
            })
            .collect::<RepositoryResult<Vec<_>>>()?
        };
        snapshot.tables.push(EntityTableSnapshot {
            table: (*table).to_string(),
            rows,
        });
    }
    Ok(snapshot)
}
/// Whether a versioned entity-table migration was already committed.
pub async fn entity_table_migration_applied(
    target: &SqlitePool,
    module: &str,
    version: i64,
) -> RepositoryResult<bool> {
    if !is_bare_ident(module) || version <= 0 {
        return Err(RepoError::query("invalid module migration marker"));
    }
    let table_exists: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'module_schema_migrations'",
    )
    .fetch_one(target)
    .await
    .map_err(map_sqlite_error)?;
    if table_exists == 0 {
        return Ok(false);
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM module_schema_migrations WHERE module = ? AND version = ?",
    )
    .bind(module)
    .bind(version)
    .fetch_one(target)
    .await
    .map_err(map_sqlite_error)?;
    Ok(count > 0)
}
/// Import an owner-provided entity snapshot atomically.
///
/// Existing identical rows are retry-safe. An ID collision with different
/// data aborts the entire migration instead of silently overwriting either
/// side's state.
#[allow(clippy::too_many_lines)]
pub async fn import_entity_tables(
    target: &SqlitePool,
    module: &str,
    version: i64,
    expected_tables: &[&str],
    snapshot: EntityStorageSnapshot,
) -> RepositoryResult<ModuleMigrationReport> {
    if !is_bare_ident(module)
        || version <= 0
        || expected_tables.iter().any(|table| !is_bare_ident(table))
    {
        return Err(RepoError::query(
            "invalid entity snapshot migration identifier",
        ));
    }
    if entity_table_migration_applied(target, module, version).await? {
        return Ok(ModuleMigrationReport {
            module: module.to_string(),
            version,
            applied: false,
            created: 0,
            skipped: 0,
        });
    }
    let expected = expected_tables
        .iter()
        .copied()
        .collect::<std::collections::HashSet<_>>();
    let actual = snapshot
        .tables
        .iter()
        .map(|table| table.table.as_str())
        .collect::<std::collections::HashSet<_>>();
    if expected != actual || actual.len() != snapshot.tables.len() {
        return Err(RepoError::query(
            "entity snapshot tables do not match the migration contract",
        ));
    }
    let mut tx = target.begin().await.map_err(map_sqlite_error)?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS module_schema_migrations (\
         module TEXT NOT NULL, version INTEGER NOT NULL, applied_at TEXT NOT NULL, \
         PRIMARY KEY (module, version))",
    )
    .execute(&mut *tx)
    .await
    .map_err(map_sqlite_error)?;
    let mut created = 0;
    let mut skipped = 0;
    for table in snapshot.tables {
        sqlx::query(&SqliteQueryAdapter::build_create_table(&table.table))
            .execute(&mut *tx)
            .await
            .map_err(map_sqlite_error)?;
        for row in table.rows {
            let existing = sqlx::query(&format!(
                "SELECT data, created_at, updated_at, deleted_at FROM {} WHERE id = ?",
                table.table
            ))
            .bind(&row.id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_sqlite_error)?;
            if let Some(existing) = existing {
                let identical = existing
                    .try_get::<String, _>("data")
                    .map_err(map_sqlite_error)?
                    == row.data
                    && existing
                        .try_get::<String, _>("created_at")
                        .map_err(map_sqlite_error)?
                        == row.created_at
                    && existing
                        .try_get::<String, _>("updated_at")
                        .map_err(map_sqlite_error)?
                        == row.updated_at
                    && existing
                        .try_get::<Option<String>, _>("deleted_at")
                        .map_err(map_sqlite_error)?
                        == row.deleted_at;
                if !identical {
                    return Err(RepoError::query(format!(
                        "entity snapshot conflict at {}:{}",
                        table.table, row.id
                    )));
                }
                skipped += 1;
                continue;
            }
            sqlx::query(&format!(
                "INSERT INTO {} (id, data, created_at, updated_at, deleted_at) \
                 VALUES (?, ?, ?, ?, ?)",
                table.table
            ))
            .bind(row.id)
            .bind(row.data)
            .bind(row.created_at)
            .bind(row.updated_at)
            .bind(row.deleted_at)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlite_error)?;
            created += 1;
        }
    }
    sqlx::query(
        "INSERT INTO module_schema_migrations (module, version, applied_at) \
         VALUES (?, ?, datetime('now'))",
    )
    .bind(module)
    .bind(version)
    .execute(&mut *tx)
    .await
    .map_err(map_sqlite_error)?;
    tx.commit().await.map_err(map_sqlite_error)?;
    Ok(ModuleMigrationReport {
        module: module.to_string(),
        version,
        applied: true,
        created,
        skipped,
    })
}
/// Repeatably copy entity rows without a module migration marker.
///
/// This is for compatibility-mirror adoption where the source may receive new
/// rows after an earlier boot. Target rows are never overwritten; ID collisions
/// are counted as `skipped`, matching [`migrate_entity_tables`].
pub async fn sync_entity_tables(
    source: Option<&SqlitePool>,
    target: &SqlitePool,
    tables: &[&str],
) -> RepositoryResult<EntityTableSyncReport> {
    if tables.iter().any(|table| !is_bare_ident(table)) {
        return Err(RepoError::query("invalid entity sync table name"));
    }
    let mut tx = target.begin().await.map_err(map_sqlite_error)?;
    let mut created = 0;
    let mut skipped = 0;
    for table in tables {
        sqlx::query(&SqliteQueryAdapter::build_create_table(table))
            .execute(&mut *tx)
            .await
            .map_err(map_sqlite_error)?;
        let Some(source) = source else { continue };
        let source_exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(*table)
        .fetch_one(source)
        .await
        .map_err(map_sqlite_error)?;
        if source_exists == 0 {
            continue;
        }
        let rows = sqlx::query(&format!(
            "SELECT id, data, created_at, updated_at, deleted_at FROM {table}"
        ))
        .fetch_all(source)
        .await
        .map_err(map_sqlite_error)?;
        for row in rows {
            let result = sqlx::query(&format!(
                "INSERT OR IGNORE INTO {table} \
                 (id, data, created_at, updated_at, deleted_at) VALUES (?, ?, ?, ?, ?)"
            ))
            .bind(row.try_get::<String, _>("id").map_err(map_sqlite_error)?)
            .bind(row.try_get::<String, _>("data").map_err(map_sqlite_error)?)
            .bind(
                row.try_get::<String, _>("created_at")
                    .map_err(map_sqlite_error)?,
            )
            .bind(
                row.try_get::<String, _>("updated_at")
                    .map_err(map_sqlite_error)?,
            )
            .bind(
                row.try_get::<Option<String>, _>("deleted_at")
                    .map_err(map_sqlite_error)?,
            )
            .execute(&mut *tx)
            .await
            .map_err(map_sqlite_error)?;
            created += result.rows_affected();
            skipped += 1 - result.rows_affected();
        }
    }
    tx.commit().await.map_err(map_sqlite_error)?;
    Ok(EntityTableSyncReport { created, skipped })
}
