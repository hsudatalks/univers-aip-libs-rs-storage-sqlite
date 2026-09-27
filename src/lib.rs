//! # Univers Storage — SQLite backend
//!
//! An embedded SQLite implementation of [`RepositoryTrait<T, D>`] from
//! `univers-storage-traits`. Intended as the default backend for relational
//! domains (ML, Task, Agent, Scripts, …) that need entity CRUD with
//! conditional queries, pagination and optimistic locking — without the
//! operational weight of SurrealDB.
//!
//! ## Quick start
//!
//! ```no_run
//! # use univers_aip_lib_storage_sqlite::{open, SqliteRepository};
//! # use univers_aip_contracts_data::storage::RepositoryTrait;
//! # use serde::{Deserialize, Serialize};
//! # #[derive(Debug, Clone, Serialize, Deserialize)]
//! # struct User { id: String, name: String }
//! # #[derive(Debug, Clone, Serialize, Deserialize)]
//! # struct UserData { name: String }
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let pool = open("sqlite://./data/app.sqlite?mode=rwc").await?;
//! let repo = SqliteRepository::<User, UserData>::new(pool, "users");
//! // repo.create(UserData { name: "Ada".into() }).await?;
//! # Ok(()) }
//! ```
//!
//! See [`repository`] for the storage model and ID round-trip details.
pub mod error;
pub mod graph;
mod json_references;
pub mod query_adapter;
pub mod repository;
pub use error::map_sqlite_error;
pub use graph::SqliteGraphRepository;
pub use json_references::install_json_references;
pub use query_adapter::{SqliteParam, SqliteQuery, SqliteQueryAdapter};
pub use repository::{SqliteRepository, SqliteWriteCoordinator};
/// Re-export so consumers can name the
/// pool type without a direct `sqlx` dependency.
pub use sqlx::sqlite::SqlitePool;
pub use univers_aip_contracts_data::storage::{
    EntityStorageSnapshot, EntityTableSnapshot, EntityTableSnapshotRow, RepositoryError,
    RepositoryResult, RepositoryTrait,
};
/// Result of an idempotent module database migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleMigrationReport {
    pub module: String,
    pub version: i64,
    pub applied: bool,
    pub created: u64,
    pub skipped: u64,
}
/// Result of a repeatable entity-table synchronization.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EntityTableSyncReport {
    pub created: u64,
    pub skipped: u64,
}

use log::LevelFilter;
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{ConnectOptions, Connection, Row};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime};
use std::{path::Path, str::FromStr};
use univers_aip_contracts_data::storage::RepositoryError as RepoError;
const PASSIVE_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(5);
const PASSIVE_CHECKPOINT_IDLE_AFTER: Duration = Duration::from_secs(30);
const PASSIVE_CHECKPOINT_FORCE_BYTES: u64 = 64 * 1024 * 1024;
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
mod entity_snapshot;
pub use entity_snapshot::{
    entity_table_migration_applied, export_entity_tables, import_entity_tables, sync_entity_tables,
};

/// Open a SQLite database at `url` and return a shared connection pool.
///
/// `url` is a sqlx SQLite connect string, e.g.:
/// - `"sqlite://./data/app.sqlite?mode=rwc"` — file DB, created if missing
/// - `"sqlite::memory:"` — in-memory (see [`open_in_memory`] for pooled use)
///
/// The pool is configured for safe concurrent access from async tasks:
/// `create_if_missing`, a 5s busy-timeout, and WAL journal mode. SQLite's
/// request-path auto-checkpoint is disabled on every connection. One pool-owned
/// task runs a non-blocking PASSIVE checkpoint after 30 seconds without WAL
/// writes, or when the WAL exceeds 64 MiB. This keeps checkpoint filesystem I/O
/// away from active request bursts while bounding WAL growth during sustained
/// traffic. Transaction durability remains SQLite's default FULL synchronous
/// mode.
///
/// Uses 64 connections and a five-second timeout. Callers that load deployment
/// settings should pass those values explicitly to [`open_with_limits`].
pub async fn open(url: &str) -> RepositoryResult<Arc<SqlitePool>> {
    open_with_limits(url, 64, Duration::from_secs(5)).await
}

/// Open a SQLite database with explicit pool limits.
///
/// The caller owns pool sizing and timeout configuration. These explicit
/// values allow independent consumers to choose their concurrency limits.
pub async fn open_with_limits(
    url: &str,
    max_connections: u32,
    timeout: Duration,
) -> RepositoryResult<Arc<SqlitePool>> {
    let opts = SqliteConnectOptions::from_str(url)
        .map_err(|e| RepoError::connection(format!("Invalid SQLite URL '{url}': {e}")))?
        .create_if_missing(true)
        .busy_timeout(timeout)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .pragma("wal_autocheckpoint", "0");
    let wal_path = {
        let mut path = opts.get_filename().as_os_str().to_os_string();
        path.push("-wal");
        std::path::PathBuf::from(path)
    };
    // Checkpoint failures have an explicit warning below. Keep expected
    // maintenance I/O from weakening slow-query signals on request connections.
    let checkpoint_opts = opts
        .clone()
        .log_slow_statements(LevelFilter::Debug, Duration::from_secs(1));

    let pool = SqlitePoolOptions::new()
        .max_connections(max_connections.max(1))
        .idle_timeout(POOL_IDLE_TIMEOUT)
        .acquire_timeout(timeout)
        // Cold-start schema work can hold SQLite connections slightly beyond
        // SQLx's 2s default. Keep genuine saturation visible before the 5s
        // shared-store timeout without reporting healthy startup as a warning.
        .acquire_slow_threshold(timeout.min(Duration::from_secs(4)))
        .connect_with(opts)
        .await
        .map_err(map_sqlite_error)?;

    let pool = Arc::new(pool);
    spawn_passive_checkpoint_worker(Arc::downgrade(&pool), wal_path, checkpoint_opts);
    Ok(pool)
}

/// Open a small pool for periodic maintenance reads against an existing store.
///
/// Maintenance callers must surface query failures explicitly. Successful
/// scans can be delayed by startup or checkpoint I/O, so their slow-statement
/// diagnostics stay at DEBUG instead of weakening request-path warning signals.
/// The primary pool remains responsible for WAL checkpointing.
pub async fn open_maintenance_pool(
    url: &str,
    max_connections: u32,
    timeout: Duration,
) -> RepositoryResult<Arc<SqlitePool>> {
    let opts = SqliteConnectOptions::from_str(url)
        .map_err(|e| RepoError::connection(format!("Invalid SQLite URL '{url}': {e}")))?
        .create_if_missing(true)
        .busy_timeout(timeout)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .pragma("wal_autocheckpoint", "0")
        .log_slow_statements(LevelFilter::Debug, Duration::from_secs(1));
    let pool = SqlitePoolOptions::new()
        .max_connections(max_connections.max(1))
        .acquire_timeout(timeout)
        .acquire_slow_threshold(timeout.min(Duration::from_secs(4)))
        .connect_with(opts)
        .await
        .map_err(map_sqlite_error)?;
    Ok(Arc::new(pool))
}

/// Checkpoint an existing SQLite database during an owner-controlled
/// maintenance window. The caller must already hold the database owner's
/// process lock; this helper only keeps the SQL implementation inside the
/// SQLite adapter instead of leaking it into lifecycle managers.
pub async fn checkpoint_database(path: &Path) -> RepositoryResult<bool> {
    if !path.is_file() {
        return Ok(false);
    }
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let pool = open_maintenance_pool(&url, 1, Duration::from_secs(30)).await?;
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(pool.as_ref())
        .await
        .map_err(map_sqlite_error)?;
    pool.close().await;
    Ok(true)
}

fn spawn_passive_checkpoint_worker(
    pool: Weak<SqlitePool>,
    wal_path: std::path::PathBuf,
    checkpoint_opts: SqliteConnectOptions,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(PASSIVE_CHECKPOINT_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut checkpointed_through = None;
        interval.tick().await;
        loop {
            interval.tick().await;
            let Some(pool) = pool.upgrade() else {
                return;
            };
            if pool.is_closed() {
                return;
            }
            let Ok(metadata) = std::fs::metadata(&wal_path) else {
                continue;
            };
            let Ok(modified_at) = metadata.modified() else {
                continue;
            };
            if !passive_checkpoint_due(
                metadata.len(),
                modified_at,
                checkpointed_through,
                SystemTime::now(),
            ) {
                continue;
            }
            let started = Instant::now();
            match passive_checkpoint(&checkpoint_opts).await {
                Ok((busy, log_frames, checkpointed_frames)) => {
                    checkpointed_through = std::fs::metadata(&wal_path)
                        .and_then(|current| current.modified())
                        .ok()
                        .or(Some(modified_at));
                    if log_frames > 0 {
                        tracing::debug!(
                            busy,
                            log_frames,
                            checkpointed_frames,
                            elapsed_micros = started.elapsed().as_micros(),
                            "SQLite passive WAL checkpoint completed"
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "SQLite passive WAL checkpoint failed; WAL remains retryable"
                    );
                }
            }
        }
    });
}

fn passive_checkpoint_due(
    wal_bytes: u64,
    modified_at: SystemTime,
    checkpointed_through: Option<SystemTime>,
    now: SystemTime,
) -> bool {
    if checkpointed_through.is_some_and(|checkpointed| modified_at <= checkpointed) {
        return false;
    }
    let idle_for = now.duration_since(modified_at).unwrap_or_default();
    idle_for >= PASSIVE_CHECKPOINT_IDLE_AFTER || wal_bytes >= PASSIVE_CHECKPOINT_FORCE_BYTES
}

async fn passive_checkpoint(
    checkpoint_opts: &SqliteConnectOptions,
) -> RepositoryResult<(i64, i64, i64)> {
    let mut connection = SqliteConnection::connect_with(checkpoint_opts)
        .await
        .map_err(map_sqlite_error)?;
    let row = sqlx::query("PRAGMA wal_checkpoint(PASSIVE)")
        .fetch_one(&mut connection)
        .await
        .map_err(map_sqlite_error)?;
    let outcome = (
        row.try_get(0).map_err(map_sqlite_error)?,
        row.try_get(1).map_err(map_sqlite_error)?,
        row.try_get(2).map_err(map_sqlite_error)?,
    );
    connection.close().await.map_err(map_sqlite_error)?;
    Ok(outcome)
}

#[cfg(test)]
#[path = "checkpoint_policy_tests.rs"]
mod checkpoint_policy_tests;

/// Open a private in-memory database with a single connection.
///
/// A SQLite `:memory:` database is **per-connection**; a multi-connection pool
/// would give each connection its own (empty) database. This helper pins the
/// pool to `max_connections(1)` so every operation hits the same in-memory DB
/// — ideal for tests and short-lived single-process use. Each call returns a
/// fresh, isolated database.
pub async fn open_in_memory() -> RepositoryResult<Arc<SqlitePool>> {
    // `:memory:` is per-connection in SQLite; pin the pool to one connection
    // so every operation sees the same in-memory database.
    let opts = SqliteConnectOptions::new()
        .in_memory(true)
        .create_if_missing(true)
        .busy_timeout(Duration::from_secs(5))
        .foreign_keys(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts)
        .await
        .map_err(map_sqlite_error)?;

    Ok(Arc::new(pool))
}

/// Strict, idempotent creation of an entity index at boot.
///
/// The entity table is created first because repositories otherwise create
/// their tables lazily. Callers use this for non-optional query indexes whose
/// absence would violate a declared production latency contract.
pub async fn ensure_index(pool: &SqlitePool, table: &str, ddl: &str) -> RepositoryResult<()> {
    let create_table = SqliteQueryAdapter::build_create_table(table);
    sqlx::query(&create_table)
        .execute(pool)
        .await
        .map_err(map_sqlite_error)?;
    sqlx::query(ddl)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(map_sqlite_error)
}

/// Best-effort creation of an index at boot.
///
/// Executes the given `CREATE [UNIQUE] INDEX IF NOT EXISTS` DDL. Returns
/// `true` when the index exists afterwards. On failure — the interesting
/// case being a UNIQUE index over rows that already violate it — logs a
/// loud warning naming the context and continues, so boot never crashes
/// on legacy data. Callers keep any in-process protection regardless; the
/// index is the cross-process backstop, not the only line of defence.
pub async fn ensure_index_best_effort(
    pool: &SqlitePool,
    table: &str,
    ddl: &str,
    context: &str,
) -> bool {
    match ensure_index(pool, table, ddl).await {
        Ok(()) => true,
        Err(error) => {
            tracing::warn!(
                %context,
                %error,
                "index creation failed (likely pre-existing duplicate rows); \
                 continuing WITHOUT the index — in-process guards still apply. \
                 Inspect duplicates and re-create the index manually."
            );
            false
        }
    }
}

/// Verify that the pool can execute a trivial read.
pub async fn health_check(pool: &SqlitePool) -> RepositoryResult<()> {
    sqlx::query("SELECT 1")
        .execute(pool)
        .await
        .map_err(map_sqlite_error)?;
    Ok(())
}

/// Create module-owned entity tables and optionally copy legacy rows atomically.
///
/// A committed `(module, version)` marker makes retries a no-op. Target rows use
/// `INSERT OR IGNORE`, so pre-existing module-owned records are never replaced.
pub async fn migrate_entity_tables(
    source: Option<&SqlitePool>,
    target: &SqlitePool,
    module: &str,
    version: i64,
    tables: &[&str],
) -> RepositoryResult<ModuleMigrationReport> {
    if !is_bare_ident(module) || version <= 0 || tables.iter().any(|table| !is_bare_ident(table)) {
        return Err(RepoError::query(
            "invalid module migration identifier or version",
        ));
    }

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS module_schema_migrations (\
         module TEXT NOT NULL, version INTEGER NOT NULL, applied_at TEXT NOT NULL, \
         PRIMARY KEY (module, version))",
    )
    .execute(target)
    .await
    .map_err(map_sqlite_error)?;
    let already_applied: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM module_schema_migrations WHERE module = ? AND version = ?",
    )
    .bind(module)
    .bind(version)
    .fetch_one(target)
    .await
    .map_err(map_sqlite_error)?;
    if already_applied > 0 {
        return Ok(ModuleMigrationReport {
            module: module.to_string(),
            version,
            applied: false,
            created: 0,
            skipped: 0,
        });
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

/// A bare identifier guard: `[A-Za-z0-9_]+` only. Used to safely interpolate a
/// caller-supplied table/field name into SQL (no quoting path).
pub(crate) fn is_bare_ident(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Read a single field from an entity's JSON `data` column by id (cross-table
/// dynamic read). `table` and `field` must be bare identifiers; returns
/// `Ok(None)` for a non-identifier or missing row.
pub async fn read_entity_field(
    pool: &SqlitePool,
    table: &str,
    id: &str,
    field: &str,
) -> RepositoryResult<Option<serde_json::Value>> {
    if !is_bare_ident(table) || !is_bare_ident(field) || id.is_empty() {
        return Ok(None);
    }
    let sql = format!("SELECT data FROM {table} WHERE id = ?");
    let row = sqlx::query(&sql)
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(map_sqlite_error)?;
    match row {
        Some(r) => {
            let data: String = r.try_get("data").map_err(map_sqlite_error)?;
            let v: serde_json::Value = serde_json::from_str(&data)
                .map_err(|e| RepoError::serialization(format!("parse entity JSON: {e}")))?;
            Ok(v.get(field).cloned())
        }
        None => Ok(None),
    }
}

/// Does a row with primary key `id` exist in `table`? TRI-STATE.
///
/// - `Ok(Some(true))` / `Ok(Some(false))` — the table exists in THIS database
///   and the row is present / definitively absent.
/// - `Ok(None)` — **cannot determine**: `table` is not a bare identifier, `id`
///   is empty, or the table is not in this database at all. The last case is
///   the important one: a store that does not own a table knows nothing about
///   rows in it, and answering `false` there would manufacture a false
///   "definitively absent" (the orchestration store holds no business tables,
///   so every world Subject would read as missing).
///
/// Unlike [`read_entity_field`], a `Some(false)` here means the ROW is absent
/// rather than a FIELD being absent, which is what makes it safe to report a
/// dangling reference from.
pub async fn entity_exists(
    pool: &SqlitePool,
    table: &str,
    id: &str,
) -> RepositoryResult<Option<bool>> {
    if !is_bare_ident(table) || id.is_empty() {
        return Ok(None);
    }
    // Table presence first: without this check a "no such table" error is
    // indistinguishable from an absent row at the call site.
    let table_present: Option<String> =
        sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?")
            .bind(table)
            .fetch_optional(pool)
            .await
            .map_err(map_sqlite_error)?;
    if table_present.is_none() {
        return Ok(None);
    }
    let sql = format!("SELECT 1 FROM {table} WHERE id = ? LIMIT 1");
    let row = sqlx::query(&sql)
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(map_sqlite_error)?;
    Ok(Some(row.is_some()))
}

/// List `(id, name)` for up to `limit` entities in `table` (cross-table dynamic
/// read). `table` must be a bare identifier; a non-identifier yields an empty vec.
pub async fn list_entity_refs(
    pool: &SqlitePool,
    table: &str,
    limit: i64,
) -> RepositoryResult<Vec<(String, Option<String>)>> {
    if !is_bare_ident(table) {
        return Ok(Vec::new());
    }
    let sql = format!("SELECT id, json_extract(data, '$.name') AS name FROM {table} LIMIT ?");
    let rows = sqlx::query(&sql)
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(map_sqlite_error)?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let id: String = r.try_get("id").map_err(map_sqlite_error)?;
        let name: Option<String> = r.try_get("name").map_err(map_sqlite_error)?;
        out.push((id, name));
    }
    Ok(out)
}

/// Identifier segment matching the resolver's strict contract: leading
/// alpha/underscore, then alphanumeric/underscore. Stricter than
/// [`is_bare_ident`] (which permits a leading digit) so where-key validation is
/// identical across backends and unchanged from the legacy SurrealDB resolver.
pub(crate) fn is_valid_path_segment(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn value_at_path<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    path.split('.')
        .try_fold(value, |current, key| current.get(key))
}

/// Build a `WHERE json_extract(data,'$.k') = json_extract(?,'$') AND …` clause
/// over a filter map. Each key may be a dotted JSON path (e.g.
/// `source_info.source_id`); every dot-segment must be a valid identifier.
/// Values bind as their JSON serialization; both sides go through
/// `json_extract`, which unwraps scalars to native SQL values so strings,
/// numbers and booleans compare correctly (`json(?)` keeps the quoted JSON
/// text form and never equals an extracted scalar). Returns `("", [])` for an
/// empty filter; rejects an unsafe key with an error (the criterion config is
/// agent-controllable, so a bad key must surface, not silently match nothing).
fn build_json_where<S: ::std::hash::BuildHasher>(
    filter: &std::collections::HashMap<String, serde_json::Value, S>,
) -> RepositoryResult<(String, Vec<String>)> {
    let mut conds: Vec<String> = Vec::with_capacity(filter.len());
    let mut binds: Vec<String> = Vec::with_capacity(filter.len());
    for (k, v) in filter {
        if !k.split('.').all(is_valid_path_segment) {
            return Err(RepoError::query(format!(
                "rejected unsafe where-key identifier: {k:?}"
            )));
        }
        conds.push(format!(
            "json_extract(data, '$.{k}') = json_extract(?, '$')"
        ));
        binds.push(serde_json::to_string(v).unwrap_or_else(|_| "null".to_string()));
    }
    let where_sql = if conds.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conds.join(" AND "))
    };
    Ok((where_sql, binds))
}

/// Read `<field>` from the LATEST entity in `table` matching `filter` (equality
/// on each key, AND-joined; newest by `created_at`/`createdAt`). Cross-table
/// dynamic read used by the task goal-eval / lifecycle resolvers. `table` must
/// be a bare identifier; `field` and filter keys may be dotted JSON paths.
pub async fn query_latest_by_conditions<S: ::std::hash::BuildHasher>(
    pool: &SqlitePool,
    table: &str,
    filter: &std::collections::HashMap<String, serde_json::Value, S>,
    field: &str,
) -> RepositoryResult<Option<serde_json::Value>> {
    if !is_bare_ident(table) || !field.split('.').all(is_valid_path_segment) {
        return Err(RepoError::query(format!(
            "rejected unsafe table/field identifier: {table:?}/{field:?}"
        )));
    }
    let (where_sql, binds) = build_json_where(filter)?;
    // Pull the full `data` row (filtered + ordered server-side) and extract the
    // field in Rust — same shape as [`read_entity_field`], type-safe across
    // scalar/object field values.
    let sql = format!(
        "SELECT data FROM {table}{where_sql} ORDER BY COALESCE(json_extract(data, '$.created_at'), json_extract(data, '$.createdAt'), '') DESC LIMIT 1"
    );
    let mut q = sqlx::query(&sql);
    for b in &binds {
        q = q.bind(b.as_str());
    }
    let row = q.fetch_optional(pool).await.map_err(map_sqlite_error)?;
    match row {
        Some(r) => {
            let data: String = r.try_get("data").map_err(map_sqlite_error)?;
            let v: serde_json::Value = serde_json::from_str(&data)
                .map_err(|e| RepoError::serialization(format!("parse entity JSON: {e}")))?;
            Ok(value_at_path(&v, field).cloned().filter(|v| !v.is_null()))
        }
        None => Ok(None),
    }
}

/// Count entities in `table` matching `filter` (equality on each key, AND-joined).
/// Cross-table dynamic read used by the task goal-eval / lifecycle resolvers.
pub async fn count_by_conditions<S: ::std::hash::BuildHasher>(
    pool: &SqlitePool,
    table: &str,
    filter: &std::collections::HashMap<String, serde_json::Value, S>,
) -> RepositoryResult<i64> {
    if !is_bare_ident(table) {
        return Err(RepoError::query(format!(
            "rejected unsafe table identifier: {table:?}"
        )));
    }
    let (where_sql, binds) = build_json_where(filter)?;
    let sql = format!("SELECT COUNT(*) AS c FROM {table}{where_sql}");
    let mut q = sqlx::query(&sql);
    for b in &binds {
        q = q.bind(b.as_str());
    }
    let row = q.fetch_one(pool).await.map_err(map_sqlite_error)?;
    let c: i64 = row.try_get("c").map_err(map_sqlite_error)?;
    Ok(c)
}

/// List user tables in this database (storage names). Read-only metadata
/// used by the entity-probe lane to advertise table ownership.
pub async fn list_tables(pool: &SqlitePool) -> RepositoryResult<Vec<String>> {
    let rows = sqlx::query(
        "SELECT name FROM sqlite_master WHERE type = 'table' \
         AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .fetch_all(pool)
    .await
    .map_err(map_sqlite_error)?;
    rows.iter()
        .map(|row| row.try_get::<String, _>("name").map_err(map_sqlite_error))
        .collect()
}

/// Delete entities in `table` matching `filter` (equality on each key,
/// AND-joined — same predicate semantics as [`count_by_conditions`]). Returns
/// the number of rows removed. An EMPTY filter is rejected: an accidental
/// empty map must never become `DELETE FROM <table>` truncating the table.
pub async fn delete_by_conditions<S: ::std::hash::BuildHasher>(
    pool: &SqlitePool,
    table: &str,
    filter: &std::collections::HashMap<String, serde_json::Value, S>,
) -> RepositoryResult<u64> {
    if !is_bare_ident(table) {
        return Err(RepoError::query(format!(
            "rejected unsafe table identifier: {table:?}"
        )));
    }
    if filter.is_empty() {
        return Err(RepoError::query(
            "delete_by_conditions requires a non-empty filter (whole-table delete refused)",
        ));
    }
    let (where_sql, binds) = build_json_where(filter)?;
    let sql = format!("DELETE FROM {table}{where_sql}");
    let mut q = sqlx::query(&sql);
    for b in &binds {
        q = q.bind(b.as_str());
    }
    let done = q.execute(pool).await.map_err(map_sqlite_error)?;
    Ok(done.rows_affected())
}

/// Delete entities in `table` whose `data.tags` JSON array contains `tag`
/// (exact string membership via `json_each`). Returns the number of rows
/// removed. Used for stores that link by tag convention instead of a
/// structural column (e.g. `cognitive_generations` carries
/// `session:<id>` / `agent:<id>` tags from the executor's LLM choke point).
pub async fn delete_by_tag(pool: &SqlitePool, table: &str, tag: &str) -> RepositoryResult<u64> {
    if !is_bare_ident(table) {
        return Err(RepoError::query(format!(
            "rejected unsafe table identifier: {table:?}"
        )));
    }
    if tag.is_empty() {
        return Err(RepoError::query(
            "delete_by_tag requires a non-empty tag (whole-table delete refused)",
        ));
    }
    let sql = format!(
        "DELETE FROM {table} WHERE EXISTS (\
         SELECT 1 FROM json_each({table}.data, '$.tags') WHERE json_each.value = ?)"
    );
    let done = sqlx::query(&sql)
        .bind(tag)
        .execute(pool)
        .await
        .map_err(map_sqlite_error)?;
    Ok(done.rows_affected())
}

/// Find the BARE id of the newest entity in `table` whose `name` equals `name`.
/// Cross-table dynamic read used by the relation-exists resolver. Returns
/// `Ok(None)` when no such row exists. `table` must be a bare identifier.
pub async fn find_id_by_name(
    pool: &SqlitePool,
    table: &str,
    name: &str,
) -> RepositoryResult<Option<String>> {
    if !is_bare_ident(table) {
        return Err(RepoError::query(format!(
            "rejected unsafe table identifier: {table:?}"
        )));
    }
    let sql = format!(
        "SELECT id FROM {table} WHERE json_extract(data, '$.name') = ? ORDER BY json_extract(data, '$.created_at') DESC LIMIT 1"
    );
    let row = sqlx::query(&sql)
        .bind(name)
        .fetch_optional(pool)
        .await
        .map_err(map_sqlite_error)?;
    match row {
        Some(r) => {
            let id: String = r.try_get("id").map_err(map_sqlite_error)?;
            Ok(Some(id))
        }
        None => Ok(None),
    }
}
