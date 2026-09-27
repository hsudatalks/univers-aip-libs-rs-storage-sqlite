//! SQLite Repository Implementation
//!
//! `SqliteRepository<T, D>` implements [`RepositoryTrait<T, D>`] over an
//! embedded SQLite database (file or in-memory).
//!
//! ## Storage model
//!
//! One table per entity, with the full entity serialized as JSON in a `data`
//! column and storage metadata (`id`, timestamps, `version`) in dedicated
//! columns:
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS "<table>" (
//!     id         TEXT PRIMARY KEY,
//!     data       TEXT NOT NULL,          -- entity serialized as JSON (sans id)
//!     created_at TEXT NOT NULL,          -- ISO8601
//!     updated_at TEXT NOT NULL,
//!     deleted_at TEXT                    -- soft-delete marker (nullable)
//! );
//! ```
//!
//! ## ID round-trip
//!
//! `D` has no `id`; `T` does. On write we serialize `D` to JSON and store the
//! bare id in the `id` column. On read we deserialize the `data` JSON and
//! inject `"id": "<table>:<id>"`; version remains part of the JSON entity so
//! the entity reconstructs exactly as SurrealDB/PostgreSQL backends materialize it. Callers may pass
//! either a bare id or a `"table:id"` RecordId string; the table prefix is
//! stripped before lookup so both forms resolve.
//!
//! Tables are created lazily and idempotently (`CREATE TABLE IF NOT EXISTS`)
//! on first use, so a fresh database file needs no migrations.

use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::Row;

use crate::error::map_sqlite_error;
use crate::query_adapter::{SqliteParam, SqliteQueryAdapter};
use univers_aip_contracts_data::storage::{
    PagedResult, QueryBuilder, RepositoryError, RepositoryResult, RepositoryTrait,
};

/// Shared admission gate for writes targeting one embedded SQLite store.
///
/// WAL permits concurrent readers but SQLite still has one writer. Repositories
/// created from the same storage plugin share this coordinator so write bursts
/// queue in-process instead of consuming the database busy timeout.
#[derive(Clone, Default)]
pub struct SqliteWriteCoordinator {
    gate: Arc<tokio::sync::Mutex<()>>,
}

impl SqliteWriteCoordinator {
    pub async fn acquire(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.gate).lock_owned().await
    }
}

/// SQLite repository for entity type `T` (with id) and data type `D` (no id).
///
/// Construct via [`SqliteRepository::new`] / [`SqliteRepository::with_soft_delete`],
/// backed by a shared [`sqlx::SqlitePool`] (see [`crate::open`]).
pub struct SqliteRepository<T, D> {
    pool: Arc<sqlx::SqlitePool>,
    table: String,
    soft_delete: bool,
    table_ready: tokio::sync::OnceCell<()>,
    write_coordinator: SqliteWriteCoordinator,
    _phantom: PhantomData<(T, D)>,
}

impl<T, D> SqliteRepository<T, D>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
    D: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Create a repository for `table` with hard deletes.
    pub fn new(pool: Arc<sqlx::SqlitePool>, table: impl Into<String>) -> Self {
        Self::with_write_coordinator(pool, table, SqliteWriteCoordinator::default())
    }

    /// Create a repository that shares write admission with sibling tables.
    pub fn with_write_coordinator(
        pool: Arc<sqlx::SqlitePool>,
        table: impl Into<String>,
        write_coordinator: SqliteWriteCoordinator,
    ) -> Self {
        Self {
            pool,
            table: table.into(),
            soft_delete: false,
            table_ready: tokio::sync::OnceCell::new(),
            write_coordinator,
            _phantom: PhantomData,
        }
    }

    /// Create a repository with soft-delete enabled (rows are marked
    /// `deleted_at` instead of removed; reads filter them out).
    pub fn with_soft_delete(pool: Arc<sqlx::SqlitePool>, table: impl Into<String>) -> Self {
        Self::with_soft_delete_and_write_coordinator(pool, table, SqliteWriteCoordinator::default())
    }

    /// Create a soft-delete repository that shares write admission with
    /// sibling repositories from the same storage plugin.
    pub fn with_soft_delete_and_write_coordinator(
        pool: Arc<sqlx::SqlitePool>,
        table: impl Into<String>,
        write_coordinator: SqliteWriteCoordinator,
    ) -> Self {
        Self {
            pool,
            table: table.into(),
            soft_delete: true,
            table_ready: tokio::sync::OnceCell::new(),
            write_coordinator,
            _phantom: PhantomData,
        }
    }

    /// Borrow the underlying pool.
    pub fn pool(&self) -> &sqlx::SqlitePool {
        &self.pool
    }

    /// The table name this repository reads/writes.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Ensure the entity table exists once for this repository instance.
    ///
    /// Failed initialization is not retained, so a transient lock or storage
    /// failure remains retryable. Successful initialization avoids turning
    /// every read into SQLite DDL and competing with unrelated entity writes.
    pub async fn ensure_table(&self) -> RepositoryResult<()> {
        self.table_ready
            .get_or_try_init(|| async {
                let _write_permit = self.write_coordinator.acquire().await;
                let sql = SqliteQueryAdapter::build_create_table(&self.table);
                sqlx::query(&sql)
                    .execute(&*self.pool)
                    .await
                    .map(|_| ())
                    .map_err(map_sqlite_error)
            })
            .await?;
        Ok(())
    }

    /// Materialize a fetched row into `T` using this instance's table name.
    ///
    /// Reads the storage `id` / `data` / `version`, then injects the
    /// `"<table>:<id>"` RecordId string and the storage-managed `version`
    /// into the JSON object before deserializing, so the entity round-trips
    /// exactly as SurrealDB/PostgreSQL backends materialize it.
    fn materialize(&self, row: sqlx::sqlite::SqliteRow) -> RepositoryResult<T> {
        let id: String = row.try_get("id").map_err(map_sqlite_error)?;
        let data_json: String = row.try_get("data").map_err(map_sqlite_error)?;

        let mut data: serde_json::Value = serde_json::from_str(&data_json).map_err(|e| {
            RepositoryError::serialization(format!("Failed to parse stored JSON: {e}"))
        })?;

        let serde_json::Value::Object(map) = &mut data else {
            return Err(RepositoryError::serialization(
                "Stored data is not a JSON object",
            ));
        };

        // Inject the RecordId (kept in the `id` column) as a "table:id" string.
        // `version` and every other field round-trip from the entity's own JSON
        // `data` — matching SurrealDB, so entities with any version type
        // (u64 / String / …) deserialize correctly.
        let full_id = if id.contains(':') {
            id
        } else {
            format!("{}:{}", self.table, id)
        };
        map.insert("id".to_string(), serde_json::Value::String(full_id));

        serde_json::from_value(data).map_err(|e| {
            RepositoryError::serialization(format!("Failed to deserialize entity: {e}"))
        })
    }
}

/// Bind a [`SqliteParam`] into a query, returning the bound query.
fn bind_param<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    param: &'q SqliteParam,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    match param {
        SqliteParam::String(s) => query.bind(s.as_str()),
        SqliteParam::Number(n) => query.bind(*n),
        SqliteParam::Bool(b) => query.bind(*b),
        SqliteParam::Null => query.bind(None::<String>),
    }
}

/// Strip a Surreal-style `"table:id"` prefix, leaving the bare record id.
/// `rsplit(':')` takes everything after the last colon, matching the
/// SurrealDB RecordId format used across the codebase.
fn bare_id(id: &str) -> &str {
    id.rsplit(':').next().unwrap_or(id)
}

/// Serialize `D` to a JSON string for storage.
fn serialize_data<D: Serialize>(data: &D) -> RepositoryResult<String> {
    serde_json::to_string(data)
        .map_err(|e| RepositoryError::serialization(format!("Failed to serialize data: {e}")))
}

#[async_trait]
impl<T, D> RepositoryTrait<T, D> for SqliteRepository<T, D>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
    D: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn supports_server_side_query(&self) -> bool {
        true
    }

    async fn create(&self, data: D) -> RepositoryResult<T>
    where
        D: Clone,
    {
        // Fresh UUID — conflict is impossible; None would be a logic error.
        let id = uuid::Uuid::new_v4().to_string();
        self.create_with_id(&id, data).await?.ok_or_else(|| {
            RepositoryError::internal("create_with_id returned no row for a fresh UUID")
        })
    }

    async fn create_with_id(&self, id: &str, data: D) -> RepositoryResult<Option<T>> {
        self.ensure_table().await?;
        let _write_permit = self.write_coordinator.acquire().await;

        let json = serialize_data(&data)?;
        let now = chrono::Utc::now().to_rfc3339();
        let sql = SqliteQueryAdapter::build_insert(&self.table);

        // ON CONFLICT(id) DO NOTHING -> duplicate id yields no row -> Ok(None).
        let row = sqlx::query(&sql)
            .bind(bare_id(id))
            .bind(json.as_str())
            .bind(&now)
            .bind(&now)
            .fetch_optional(&*self.pool)
            .await
            .map_err(map_sqlite_error)?;

        match row {
            Some(r) => Ok(Some(self.materialize(r)?)),
            None => Ok(None),
        }
    }

    async fn find_by_id(&self, id: &str) -> RepositoryResult<Option<T>> {
        self.ensure_table().await?;

        let sql = SqliteQueryAdapter::build_select_by_id(&self.table, self.soft_delete);
        let row = sqlx::query(&sql)
            .bind(bare_id(id))
            .fetch_optional(&*self.pool)
            .await
            .map_err(map_sqlite_error)?;

        match row {
            Some(r) => Ok(Some(self.materialize(r)?)),
            None => Ok(None),
        }
    }

    async fn find_by_ids(&self, ids: Vec<&str>) -> RepositoryResult<Vec<T>> {
        self.ensure_table().await?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(1000) {
            let sql =
                SqliteQueryAdapter::build_select_in(&self.table, chunk.len(), self.soft_delete);
            let mut query = sqlx::query(&sql);
            for id in chunk {
                query = query.bind(bare_id(id));
            }
            let rows = query
                .fetch_all(&*self.pool)
                .await
                .map_err(map_sqlite_error)?;
            for row in rows {
                results.push(self.materialize(row)?);
            }
        }
        Ok(results)
    }

    async fn update(&self, id: &str, data: D) -> RepositoryResult<Option<T>>
    where
        D: Clone,
    {
        self.ensure_table().await?;
        let _write_permit = self.write_coordinator.acquire().await;

        let json = serialize_data(&data)?;
        let now = chrono::Utc::now().to_rfc3339();
        let sql = SqliteQueryAdapter::build_update(&self.table);

        let row = sqlx::query(&sql)
            .bind(json.as_str())
            .bind(&now)
            .bind(bare_id(id))
            .fetch_optional(&*self.pool)
            .await
            .map_err(map_sqlite_error)?;

        match row {
            Some(r) => Ok(Some(self.materialize(r)?)),
            None => Ok(None),
        }
    }

    async fn update_with_version(
        &self,
        id: &str,
        data: D,
        expected_version: i64,
    ) -> RepositoryResult<Option<T>>
    where
        D: Clone,
    {
        self.ensure_table().await?;
        let _write_permit = self.write_coordinator.acquire().await;

        let json = serialize_data(&data)?;
        let now = chrono::Utc::now().to_rfc3339();
        let sql = SqliteQueryAdapter::build_update_with_version(&self.table);

        let row = sqlx::query(&sql)
            .bind(json.as_str())
            .bind(&now)
            .bind(bare_id(id))
            .bind(expected_version)
            .fetch_optional(&*self.pool)
            .await
            .map_err(map_sqlite_error)?;

        match row {
            Some(r) => Ok(Some(self.materialize(r)?)),
            None => Ok(None), // version mismatch OR id absent
        }
    }

    async fn update_with_version_and_timestamp_guard(
        &self,
        id: &str,
        data: D,
        expected_version: i64,
        timestamp_field: &str,
    ) -> RepositoryResult<Option<T>>
    where
        D: Clone,
    {
        self.ensure_table().await?;
        let _write_permit = self.write_coordinator.acquire().await;

        let json = serialize_data(&data)?;
        let now = chrono::Utc::now().to_rfc3339();
        let sql = SqliteQueryAdapter::build_update_with_version_and_timestamp_guard(
            &self.table,
            timestamp_field,
        )?;

        let row = sqlx::query(&sql)
            .bind(json.as_str())
            .bind(&now)
            .bind(bare_id(id))
            .bind(expected_version)
            .fetch_optional(&*self.pool)
            .await
            .map_err(map_sqlite_error)?;

        row.map(|row| self.materialize(row)).transpose()
    }

    async fn update_with_version_and_expired_timestamp_guard(
        &self,
        id: &str,
        data: D,
        expected_version: i64,
        timestamp_field: &str,
    ) -> RepositoryResult<Option<T>>
    where
        D: Clone,
    {
        self.ensure_table().await?;
        let _write_permit = self.write_coordinator.acquire().await;

        let json = serialize_data(&data)?;
        let now = chrono::Utc::now().to_rfc3339();
        let sql = SqliteQueryAdapter::build_update_with_version_and_expired_timestamp_guard(
            &self.table,
            timestamp_field,
        )?;

        let row = sqlx::query(&sql)
            .bind(json.as_str())
            .bind(&now)
            .bind(bare_id(id))
            .bind(expected_version)
            .fetch_optional(&*self.pool)
            .await
            .map_err(map_sqlite_error)?;

        row.map(|row| self.materialize(row)).transpose()
    }

    async fn delete(&self, id: &str) -> RepositoryResult<Option<T>> {
        self.ensure_table().await?;
        let _write_permit = self.write_coordinator.acquire().await;

        let row = if self.soft_delete {
            let now = chrono::Utc::now().to_rfc3339();
            let sql = SqliteQueryAdapter::build_soft_delete(&self.table);
            sqlx::query(&sql)
                .bind(&now)
                .bind(bare_id(id))
                .fetch_optional(&*self.pool)
                .await
                .map_err(map_sqlite_error)?
        } else {
            let sql = SqliteQueryAdapter::build_delete(&self.table);
            sqlx::query(&sql)
                .bind(bare_id(id))
                .fetch_optional(&*self.pool)
                .await
                .map_err(map_sqlite_error)?
        };

        match row {
            Some(r) => Ok(Some(self.materialize(r)?)),
            None => Ok(None),
        }
    }

    async fn delete_with_version(
        &self,
        id: &str,
        expected_version: i64,
    ) -> RepositoryResult<Option<T>> {
        self.ensure_table().await?;
        let _write_permit = self.write_coordinator.acquire().await;

        let row = if self.soft_delete {
            let now = chrono::Utc::now().to_rfc3339();
            let sql = SqliteQueryAdapter::build_soft_delete_with_version(&self.table);
            sqlx::query(&sql)
                .bind(&now)
                .bind(bare_id(id))
                .bind(expected_version)
                .fetch_optional(&*self.pool)
                .await
                .map_err(map_sqlite_error)?
        } else {
            let sql = SqliteQueryAdapter::build_delete_with_version(&self.table);
            sqlx::query(&sql)
                .bind(bare_id(id))
                .bind(expected_version)
                .fetch_optional(&*self.pool)
                .await
                .map_err(map_sqlite_error)?
        };

        match row {
            Some(r) => Ok(Some(self.materialize(r)?)),
            None => Ok(None),
        }
    }

    async fn list(&self) -> RepositoryResult<Vec<T>> {
        self.ensure_table().await?;
        let sql = SqliteQueryAdapter::build_select_all(&self.table, self.soft_delete);
        let rows = sqlx::query(&sql)
            .fetch_all(&*self.pool)
            .await
            .map_err(map_sqlite_error)?;
        rows.into_iter().map(|r| self.materialize(r)).collect()
    }

    async fn count(&self) -> RepositoryResult<usize> {
        self.ensure_table().await?;
        let sql = SqliteQueryAdapter::build_count(&self.table, self.soft_delete);
        let row = sqlx::query(&sql)
            .fetch_one(&*self.pool)
            .await
            .map_err(map_sqlite_error)?;
        let c: i64 = row.try_get("count").map_err(map_sqlite_error)?;
        Ok(c as usize)
    }

    async fn query_safe(&self, builder: QueryBuilder) -> RepositoryResult<Vec<T>> {
        self.ensure_table().await?;
        let q = SqliteQueryAdapter::build_select(&self.table, &builder, self.soft_delete);

        let mut query = sqlx::query(&q.sql);
        for param in &q.params {
            query = bind_param(query, param);
        }

        let rows = query
            .fetch_all(&*self.pool)
            .await
            .map_err(map_sqlite_error)?;
        rows.into_iter().map(|r| self.materialize(r)).collect()
    }

    async fn list_paginated(
        &self,
        page: usize,
        page_size: usize,
    ) -> RepositoryResult<PagedResult<T>> {
        let total = self.count().await?;
        let total_pages = if page_size == 0 {
            0
        } else {
            total.div_ceil(page_size)
        };

        if page_size == 0 || total == 0 {
            return Ok(PagedResult {
                items: Vec::new(),
                page,
                page_size,
                total,
                total_pages,
            });
        }

        let builder = QueryBuilder::new()
            .limit(page_size)
            .offset(page * page_size);
        let items = self.query_safe(builder).await?;

        Ok(PagedResult {
            items,
            page,
            page_size,
            total,
            total_pages,
        })
    }

    async fn create_batch(&self, data: Vec<D>) -> RepositoryResult<Vec<T>>
    where
        D: Clone,
    {
        if data.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_table().await?;
        let _write_permit = self.write_coordinator.acquire().await;

        let sql = SqliteQueryAdapter::build_insert(&self.table);
        let mut tx = self.pool.begin().await.map_err(map_sqlite_error)?;
        let mut results = Vec::with_capacity(data.len());

        for item in data {
            let id = uuid::Uuid::new_v4().to_string();
            let json = serialize_data(&item)?;
            let now = chrono::Utc::now().to_rfc3339();

            let row = sqlx::query(&sql)
                .bind(&id)
                .bind(json.as_str())
                .bind(&now)
                .bind(&now)
                .fetch_one(&mut *tx)
                .await
                .map_err(map_sqlite_error)?;

            results.push(self.materialize(row)?);
        }

        tx.commit().await.map_err(map_sqlite_error)?;
        Ok(results)
    }

    async fn delete_batch(&self, ids: Vec<&str>) -> RepositoryResult<Vec<Option<T>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_table().await?;
        let _write_permit = self.write_coordinator.acquire().await;
        let mut tx = self.pool.begin().await.map_err(map_sqlite_error)?;
        let mut results = Vec::with_capacity(ids.len());
        if self.soft_delete {
            let sql = SqliteQueryAdapter::build_soft_delete(&self.table);
            let now = chrono::Utc::now().to_rfc3339();
            for id in ids {
                let row = sqlx::query(&sql)
                    .bind(&now)
                    .bind(bare_id(id))
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(map_sqlite_error)?;
                results.push(row.map(|row| self.materialize(row)).transpose()?);
            }
        } else {
            let sql = SqliteQueryAdapter::build_delete(&self.table);
            for id in ids {
                let row = sqlx::query(&sql)
                    .bind(bare_id(id))
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(map_sqlite_error)?;
                results.push(row.map(|row| self.materialize(row)).transpose()?);
            }
        }
        tx.commit().await.map_err(map_sqlite_error)?;
        Ok(results)
    }

    async fn count_filtered(&self, builder: QueryBuilder) -> RepositoryResult<usize> {
        self.ensure_table().await?;
        let q = SqliteQueryAdapter::build_count_filtered(&self.table, &builder, self.soft_delete);

        let mut query = sqlx::query(&q.sql);
        for param in &q.params {
            query = bind_param(query, param);
        }

        let row = query
            .fetch_one(&*self.pool)
            .await
            .map_err(map_sqlite_error)?;
        let c: i64 = row.try_get("count").map_err(map_sqlite_error)?;
        Ok(c as usize)
    }
}
