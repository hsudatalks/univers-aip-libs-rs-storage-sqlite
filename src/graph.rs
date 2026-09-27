//! SQLite GraphRepository — relation edges in a single `relation_edges` table.
//!
//! Semantics mirror the `univers-storage-kv-graph` adapter:
//! - `relate` is idempotent and returns `Ok(true)` only when a new edge was
//!   created (`false` if it already existed).
//! - `unrelate` / `unrelate_batch` are idempotent deletes.
//! - Edge metadata is stored as JSON text in the `metadata` column.
//! - `sweep_entity` removes every edge touching an entity (both directions)
//!   in bounded chunks and returns the swept-edge count.
//!
//! ## Storage model
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS relation_edges (
//!     src_table TEXT NOT NULL,
//!     src_id    TEXT NOT NULL,
//!     relation  TEXT NOT NULL,
//!     tgt_table TEXT NOT NULL,
//!     tgt_id    TEXT NOT NULL,
//!     metadata  TEXT,
//!     PRIMARY KEY (src_table, src_id, relation, tgt_table, tgt_id)
//! );
//! ```
//!
//! The composite primary key doubles as the forward index (`find_targets`);
//! `idx_relation_edges_reverse (tgt_table, tgt_id, relation)` serves
//! `find_sources`, and `idx_relation_edges_relation (relation)` serves
//! `find_by_type` / `get_stats`. Table and relation names are always **bound
//! values**, never interpolated identifiers — there is no injection surface.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;

use async_trait::async_trait;
use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use sqlx::{Sqlite, Transaction};

use crate::error::map_sqlite_error;
use univers_aip_contracts_data::storage::graph_repository::{
    RelationProjectionFence, RelationProjectionMutation,
};
use univers_aip_contracts_data::storage::{
    GraphRelationStats, GraphRepository, GraphRepositoryError, RelationMetadata,
};

/// Deterministic ordering for listing queries: full edge key.
const EDGE_ORDER: &str = "ORDER BY src_table, src_id, relation, tgt_table, tgt_id";

/// Chunk size for `sweep_entity` deletes — bounds each write transaction so a
/// hub entity never holds the writer lock for one giant delete.
const SWEEP_CHUNK: usize = 512;
const PROJECTION_FENCE_KEY: &str = "_univers_projection_fence";
const PROJECTION_TOMBSTONE_PREFIX: &str = "_univers_projection_tombstone:";

/// A `GraphRepository` backed by SQLite (`relation_edges` table).
///
/// This adapter backs the **intra-cluster** edges of the intelligence store
/// (task / agents / cognitive / llm) per `docs/ORCH_STORAGE_MIGRATION.md`'s
/// amendment: entities and their edges live in the *same* SQLite file, so
/// entity + edge writes are ACID together. Ontology and mixed/cross-cluster
/// edges stay on the shared KV graph (`univers-storage-kv-graph`).
pub struct SqliteGraphRepository {
    pool: Arc<SqlitePool>,
}

impl SqliteGraphRepository {
    /// Create the repository and ensure the edge table + indexes exist
    /// (idempotent `CREATE TABLE IF NOT EXISTS`).
    pub async fn new(pool: Arc<SqlitePool>) -> Result<Self, GraphRepositoryError> {
        let repo = Self { pool };
        repo.ensure_schema().await?;
        Ok(repo)
    }

    /// Borrow the underlying pool.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Idempotently create the `relation_edges` table and its indexes.
    pub async fn ensure_schema(&self) -> Result<(), GraphRepositoryError> {
        sqlx::query(
            r"CREATE TABLE IF NOT EXISTS relation_edges (
                src_table TEXT NOT NULL,
                src_id    TEXT NOT NULL,
                relation  TEXT NOT NULL,
                tgt_table TEXT NOT NULL,
                tgt_id    TEXT NOT NULL,
                metadata  TEXT,
                PRIMARY KEY (src_table, src_id, relation, tgt_table, tgt_id)
            )",
        )
        .execute(&*self.pool)
        .await
        .map_err(Self::gerr)?;
        sqlx::query(
            r"CREATE INDEX IF NOT EXISTS idx_relation_edges_reverse
              ON relation_edges (tgt_table, tgt_id, relation)",
        )
        .execute(&*self.pool)
        .await
        .map_err(Self::gerr)?;
        sqlx::query(
            r"CREATE INDEX IF NOT EXISTS idx_relation_edges_relation
              ON relation_edges (relation)",
        )
        .execute(&*self.pool)
        .await
        .map_err(Self::gerr)?;
        Ok(())
    }

    /// Map an sqlx error through the crate's classifier into the graph error type.
    fn gerr(e: sqlx::Error) -> GraphRepositoryError {
        GraphRepositoryError::Storage(map_sqlite_error(e).to_string())
    }

    /// `?, ?, …` placeholder list for an `IN (…)` clause of `n` bound values.
    fn placeholders(n: usize) -> String {
        vec!["?"; n].join(", ")
    }

    /// True when the error is SQLite's "no such table" — entity tables are
    /// created lazily by `SqliteRepository`, so a missing table simply means
    /// "no records yet" for the read-only entity operations.
    fn is_missing_table(e: &sqlx::Error) -> bool {
        matches!(e, sqlx::Error::Database(db) if db.message().contains("no such table"))
    }

    /// Parse a stored `metadata` column into the trait's metadata map.
    /// `NULL` or non-object JSON yields `None` (same as the KV adapter, where
    /// an empty value or unparsable payload reads back as `None`).
    fn parse_metadata(raw: Option<String>) -> Option<RelationMetadata> {
        raw.and_then(|s| serde_json::from_str::<RelationMetadata>(&s).ok())
    }

    fn projection_tombstone_relation(relation: &str) -> Result<String, GraphRepositoryError> {
        if relation.starts_with(PROJECTION_TOMBSTONE_PREFIX) {
            return Err(GraphRepositoryError::InvalidQuery(
                "reserved relation projection tombstone namespace".to_string(),
            ));
        }
        Ok(format!("{PROJECTION_TOMBSTONE_PREFIX}{relation}"))
    }

    fn legacy_projection_mutation_error() -> GraphRepositoryError {
        GraphRepositoryError::ConstraintViolation(
            "legacy relation mutation cannot modify a fenced projection".to_string(),
        )
    }

    fn metadata_has_projection_fence(raw: Option<&str>) -> bool {
        raw.and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
            .and_then(|value| value.as_object().cloned())
            .is_some_and(|metadata| metadata.contains_key(PROJECTION_FENCE_KEY))
    }

    async fn ensure_legacy_edge_unfenced(
        transaction: &mut Transaction<'_, Sqlite>,
        source_table: &str,
        source_id: &str,
        target_table: &str,
        target_id: &str,
        relation: &str,
    ) -> Result<(), GraphRepositoryError> {
        let tombstone_relation = Self::projection_tombstone_relation(relation)?;
        let rows: Vec<(String, Option<String>)> = sqlx::query_as(
            r"SELECT relation, metadata FROM relation_edges
              WHERE src_table = ? AND src_id = ? AND tgt_table = ? AND tgt_id = ?
                AND relation IN (?, ?)",
        )
        .bind(source_table)
        .bind(source_id)
        .bind(target_table)
        .bind(target_id)
        .bind(relation)
        .bind(&tombstone_relation)
        .fetch_all(&mut **transaction)
        .await
        .map_err(Self::gerr)?;
        if rows.iter().any(|(stored_relation, metadata)| {
            stored_relation == &tombstone_relation
                || Self::metadata_has_projection_fence(metadata.as_deref())
        }) {
            return Err(Self::legacy_projection_mutation_error());
        }
        Ok(())
    }

    async fn ensure_legacy_entity_unfenced(
        transaction: &mut Transaction<'_, Sqlite>,
        table: &str,
        id: &str,
        relation_names: &[&str],
    ) -> Result<(), GraphRepositoryError> {
        for relation in relation_names {
            Self::projection_tombstone_relation(relation)?;
        }
        let rows: Vec<(String, Option<String>)> = sqlx::query_as(
            r"SELECT relation, metadata FROM relation_edges
              WHERE (src_table = ? AND src_id = ?) OR (tgt_table = ? AND tgt_id = ?)",
        )
        .bind(table)
        .bind(id)
        .bind(table)
        .bind(id)
        .fetch_all(&mut **transaction)
        .await
        .map_err(Self::gerr)?;
        let has_fenced_projection = rows.iter().any(|(relation, metadata)| {
            let projected_relation = relation
                .strip_prefix(PROJECTION_TOMBSTONE_PREFIX)
                .unwrap_or(relation);
            let selected =
                relation_names.is_empty() || relation_names.contains(&projected_relation);
            selected
                && (relation.starts_with(PROJECTION_TOMBSTONE_PREFIX)
                    || Self::metadata_has_projection_fence(metadata.as_deref()))
        });
        if has_fenced_projection {
            return Err(Self::legacy_projection_mutation_error());
        }
        Ok(())
    }

    fn encode_projection(
        mut metadata: RelationMetadata,
        fence: &RelationProjectionFence,
        deleted: bool,
    ) -> Result<String, GraphRepositoryError> {
        if metadata.contains_key(PROJECTION_FENCE_KEY) {
            return Err(GraphRepositoryError::ConstraintViolation(format!(
                "relation metadata field '{PROJECTION_FENCE_KEY}' is reserved"
            )));
        }
        metadata.insert(
            PROJECTION_FENCE_KEY.to_string(),
            serde_json::json!({
                "owner": fence.owner,
                "revision": fence.revision,
                "deleted": deleted,
            }),
        );
        serde_json::to_string(&metadata)
            .map_err(|error| GraphRepositoryError::Storage(error.to_string()))
    }

    fn decode_projection(
        raw: Option<&str>,
    ) -> Result<(RelationProjectionFence, RelationMetadata, bool), GraphRepositoryError> {
        let raw = raw.ok_or_else(|| {
            GraphRepositoryError::ConstraintViolation(
                "cannot conditionally mutate an unfenced relation edge".to_string(),
            )
        })?;
        let mut metadata: RelationMetadata = serde_json::from_str(raw).map_err(|error| {
            GraphRepositoryError::ConstraintViolation(format!(
                "relation projection has invalid metadata: {error}"
            ))
        })?;
        let raw_fence = metadata.remove(PROJECTION_FENCE_KEY).ok_or_else(|| {
            GraphRepositoryError::ConstraintViolation(
                "cannot conditionally mutate an unfenced relation edge".to_string(),
            )
        })?;
        let deleted = raw_fence
            .get("deleted")
            .and_then(serde_json::Value::as_bool)
            .ok_or_else(|| {
                GraphRepositoryError::ConstraintViolation(
                    "relation projection fence is missing its deletion state".to_string(),
                )
            })?;
        let fence = serde_json::from_value(raw_fence).map_err(|error| {
            GraphRepositoryError::ConstraintViolation(format!(
                "relation projection has an invalid fence: {error}"
            ))
        })?;
        Ok((fence, metadata, deleted))
    }

    fn compare_upsert_projection(
        current: &RelationProjectionFence,
        requested: &RelationProjectionFence,
    ) -> Option<RelationProjectionMutation> {
        if current.owner != requested.owner && current.revision >= requested.revision {
            Some(RelationProjectionMutation::OwnershipMismatch {
                current: current.clone(),
            })
        } else if current.revision > requested.revision {
            Some(RelationProjectionMutation::Superseded {
                current: current.clone(),
            })
        } else {
            None
        }
    }

    fn compare_delete_projection(
        current: &RelationProjectionFence,
        requested: &RelationProjectionFence,
    ) -> Option<RelationProjectionMutation> {
        if current.owner != requested.owner {
            Some(RelationProjectionMutation::OwnershipMismatch {
                current: current.clone(),
            })
        } else if current.revision > requested.revision {
            Some(RelationProjectionMutation::Superseded {
                current: current.clone(),
            })
        } else {
            None
        }
    }

    /// Fetch `(table, id)` pairs with optional LIMIT/OFFSET.
    async fn fetch_pairs(
        &self,
        sql: &str,
        binds: &[&str],
        page: Option<(usize, usize)>,
    ) -> Result<Vec<(String, String)>, GraphRepositoryError> {
        let mut q = sqlx::query_as::<_, (String, String)>(sql);
        for b in binds {
            q = q.bind(*b);
        }
        if let Some((limit, offset)) = page {
            q = q.bind(limit as i64).bind(offset as i64);
        }
        q.fetch_all(&*self.pool).await.map_err(Self::gerr)
    }
}

#[async_trait]
impl GraphRepository for SqliteGraphRepository {
    // ── Core CRUD ───────────────────────────────────────────

    async fn relate(
        &self,
        source_table: &str,
        source_id: &str,
        target_table: &str,
        target_id: &str,
        relation: &str,
    ) -> Result<bool, GraphRepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(Self::gerr)?;
        Self::ensure_legacy_edge_unfenced(
            &mut transaction,
            source_table,
            source_id,
            target_table,
            target_id,
            relation,
        )
        .await?;
        let res = sqlx::query(
            r"INSERT INTO relation_edges (src_table, src_id, relation, tgt_table, tgt_id, metadata)
              VALUES (?, ?, ?, ?, ?, NULL)
              ON CONFLICT (src_table, src_id, relation, tgt_table, tgt_id) DO NOTHING",
        )
        .bind(source_table)
        .bind(source_id)
        .bind(relation)
        .bind(target_table)
        .bind(target_id)
        .execute(&mut *transaction)
        .await
        .map_err(Self::gerr)?;
        transaction.commit().await.map_err(Self::gerr)?;
        Ok(res.rows_affected() > 0)
    }

    async fn relate_with_metadata(
        &self,
        source_table: &str,
        source_id: &str,
        target_table: &str,
        target_id: &str,
        relation: &str,
        metadata: serde_json::Value,
    ) -> Result<(), GraphRepositoryError> {
        // Upsert: like the KV adapter, an existing edge gets its metadata replaced.
        let meta = serde_json::to_string(&metadata).unwrap_or_default();
        let mut transaction = self.pool.begin().await.map_err(Self::gerr)?;
        Self::ensure_legacy_edge_unfenced(
            &mut transaction,
            source_table,
            source_id,
            target_table,
            target_id,
            relation,
        )
        .await?;
        sqlx::query(
            r"INSERT INTO relation_edges (src_table, src_id, relation, tgt_table, tgt_id, metadata)
              VALUES (?, ?, ?, ?, ?, ?)
              ON CONFLICT (src_table, src_id, relation, tgt_table, tgt_id)
              DO UPDATE SET metadata = excluded.metadata",
        )
        .bind(source_table)
        .bind(source_id)
        .bind(relation)
        .bind(target_table)
        .bind(target_id)
        .bind(meta)
        .execute(&mut *transaction)
        .await
        .map_err(Self::gerr)?;
        transaction.commit().await.map_err(Self::gerr)?;
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn upsert_relation_projection(
        &self,
        source_table: &str,
        source_id: &str,
        target_table: &str,
        target_id: &str,
        relation: &str,
        metadata: RelationMetadata,
        fence: RelationProjectionFence,
    ) -> Result<RelationProjectionMutation, GraphRepositoryError> {
        let tombstone_relation = Self::projection_tombstone_relation(relation)?;
        let live_metadata = Self::encode_projection(metadata.clone(), &fence, false)?;

        for _ in 0..64 {
            let mut transaction = self.pool.begin().await.map_err(Self::gerr)?;
            let rows: Vec<(String, Option<String>)> = sqlx::query_as(
                r"SELECT relation, metadata FROM relation_edges
                  WHERE src_table = ? AND src_id = ? AND tgt_table = ? AND tgt_id = ?
                    AND relation IN (?, ?)",
            )
            .bind(source_table)
            .bind(source_id)
            .bind(target_table)
            .bind(target_id)
            .bind(relation)
            .bind(&tombstone_relation)
            .fetch_all(&mut *transaction)
            .await
            .map_err(Self::gerr)?;
            if rows.len() > 1 {
                return Err(GraphRepositoryError::ConstraintViolation(
                    "relation projection has both a live edge and tombstone".to_string(),
                ));
            }

            let current = rows.first();
            if let Some((stored_relation, raw)) = current {
                let (current_fence, current_metadata, deleted) =
                    Self::decode_projection(raw.as_deref())?;
                if deleted != (stored_relation == &tombstone_relation) {
                    return Err(GraphRepositoryError::ConstraintViolation(
                        "relation projection fence has an inconsistent deletion state".to_string(),
                    ));
                }
                if let Some(outcome) = Self::compare_upsert_projection(&current_fence, &fence) {
                    return Ok(outcome);
                }
                if current_fence.revision == fence.revision {
                    if !deleted && current_metadata == metadata {
                        return Ok(RelationProjectionMutation::AlreadyApplied);
                    }
                    return Err(GraphRepositoryError::ConstraintViolation(
                        "equal relation projection fence is bound to a different mutation"
                            .to_string(),
                    ));
                }
            }

            let changed = match current {
                None => {
                    sqlx::query(
                        r"INSERT INTO relation_edges
                          (src_table, src_id, relation, tgt_table, tgt_id, metadata)
                          VALUES (?, ?, ?, ?, ?, ?)
                          ON CONFLICT (src_table, src_id, relation, tgt_table, tgt_id) DO NOTHING",
                    )
                    .bind(source_table)
                    .bind(source_id)
                    .bind(relation)
                    .bind(target_table)
                    .bind(target_id)
                    .bind(&live_metadata)
                    .execute(&mut *transaction)
                    .await
                    .map_err(Self::gerr)?
                    .rows_affected()
                        == 1
                }
                Some((stored_relation, raw)) if stored_relation == relation => {
                    sqlx::query(
                        r"UPDATE relation_edges SET metadata = ?
                          WHERE src_table = ? AND src_id = ? AND relation = ?
                            AND tgt_table = ? AND tgt_id = ? AND metadata IS ?",
                    )
                    .bind(&live_metadata)
                    .bind(source_table)
                    .bind(source_id)
                    .bind(relation)
                    .bind(target_table)
                    .bind(target_id)
                    .bind(raw)
                    .execute(&mut *transaction)
                    .await
                    .map_err(Self::gerr)?
                    .rows_affected()
                        == 1
                }
                Some((_, raw)) => {
                    let deleted = sqlx::query(
                        r"DELETE FROM relation_edges
                          WHERE src_table = ? AND src_id = ? AND relation = ?
                            AND tgt_table = ? AND tgt_id = ? AND metadata IS ?",
                    )
                    .bind(source_table)
                    .bind(source_id)
                    .bind(&tombstone_relation)
                    .bind(target_table)
                    .bind(target_id)
                    .bind(raw)
                    .execute(&mut *transaction)
                    .await
                    .map_err(Self::gerr)?
                    .rows_affected()
                        == 1;
                    if !deleted {
                        false
                    } else {
                        sqlx::query(
                            r"INSERT INTO relation_edges
                              (src_table, src_id, relation, tgt_table, tgt_id, metadata)
                              VALUES (?, ?, ?, ?, ?, ?)
                              ON CONFLICT (src_table, src_id, relation, tgt_table, tgt_id) DO NOTHING",
                        )
                        .bind(source_table)
                        .bind(source_id)
                        .bind(relation)
                        .bind(target_table)
                        .bind(target_id)
                        .bind(&live_metadata)
                        .execute(&mut *transaction)
                        .await
                        .map_err(Self::gerr)?
                        .rows_affected()
                            == 1
                    }
                }
            };
            if changed {
                transaction.commit().await.map_err(Self::gerr)?;
                return Ok(RelationProjectionMutation::Applied);
            }
            transaction.rollback().await.map_err(Self::gerr)?;
        }
        Err(GraphRepositoryError::Storage(
            "relation projection upsert exhausted conditional retries".to_string(),
        ))
    }

    async fn update_relation_metadata(
        &self,
        source_table: &str,
        source_id: &str,
        target_table: &str,
        target_id: &str,
        relation: &str,
        metadata: serde_json::Value,
    ) -> Result<(), GraphRepositoryError> {
        let meta = serde_json::to_string(&metadata).unwrap_or_default();
        let mut transaction = self.pool.begin().await.map_err(Self::gerr)?;
        Self::ensure_legacy_edge_unfenced(
            &mut transaction,
            source_table,
            source_id,
            target_table,
            target_id,
            relation,
        )
        .await?;
        let res = sqlx::query(
            r"UPDATE relation_edges SET metadata = ?
              WHERE src_table = ? AND src_id = ? AND relation = ?
                AND tgt_table = ? AND tgt_id = ?",
        )
        .bind(meta)
        .bind(source_table)
        .bind(source_id)
        .bind(relation)
        .bind(target_table)
        .bind(target_id)
        .execute(&mut *transaction)
        .await
        .map_err(Self::gerr)?;
        if res.rows_affected() == 0 {
            return Err(GraphRepositoryError::NotFound(format!(
                "relation {source_table}:{source_id} -[{relation}]-> {target_table}:{target_id}"
            )));
        }
        transaction.commit().await.map_err(Self::gerr)?;
        Ok(())
    }

    async fn unrelate(
        &self,
        source_table: &str,
        source_id: &str,
        target_table: &str,
        target_id: &str,
        relation: &str,
    ) -> Result<(), GraphRepositoryError> {
        // Idempotent: deleting a non-existent edge is a no-op.
        let mut transaction = self.pool.begin().await.map_err(Self::gerr)?;
        Self::ensure_legacy_edge_unfenced(
            &mut transaction,
            source_table,
            source_id,
            target_table,
            target_id,
            relation,
        )
        .await?;
        sqlx::query(
            r"DELETE FROM relation_edges
              WHERE src_table = ? AND src_id = ? AND relation = ?
                AND tgt_table = ? AND tgt_id = ?",
        )
        .bind(source_table)
        .bind(source_id)
        .bind(relation)
        .bind(target_table)
        .bind(target_id)
        .execute(&mut *transaction)
        .await
        .map_err(Self::gerr)?;
        transaction.commit().await.map_err(Self::gerr)?;
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn delete_relation_projection(
        &self,
        source_table: &str,
        source_id: &str,
        target_table: &str,
        target_id: &str,
        relation: &str,
        fence: RelationProjectionFence,
    ) -> Result<RelationProjectionMutation, GraphRepositoryError> {
        let tombstone_relation = Self::projection_tombstone_relation(relation)?;
        let tombstone_metadata = Self::encode_projection(RelationMetadata::new(), &fence, true)?;

        for _ in 0..64 {
            let mut transaction = self.pool.begin().await.map_err(Self::gerr)?;
            let rows: Vec<(String, Option<String>)> = sqlx::query_as(
                r"SELECT relation, metadata FROM relation_edges
                  WHERE src_table = ? AND src_id = ? AND tgt_table = ? AND tgt_id = ?
                    AND relation IN (?, ?)",
            )
            .bind(source_table)
            .bind(source_id)
            .bind(target_table)
            .bind(target_id)
            .bind(relation)
            .bind(&tombstone_relation)
            .fetch_all(&mut *transaction)
            .await
            .map_err(Self::gerr)?;
            if rows.len() > 1 {
                return Err(GraphRepositoryError::ConstraintViolation(
                    "relation projection has both a live edge and tombstone".to_string(),
                ));
            }

            let current = rows.first();
            if let Some((stored_relation, raw)) = current {
                let (current_fence, _, deleted) = Self::decode_projection(raw.as_deref())?;
                if deleted != (stored_relation == &tombstone_relation) {
                    return Err(GraphRepositoryError::ConstraintViolation(
                        "relation projection fence has an inconsistent deletion state".to_string(),
                    ));
                }
                if let Some(outcome) = Self::compare_delete_projection(&current_fence, &fence) {
                    return Ok(outcome);
                }
                if current_fence.revision == fence.revision {
                    if deleted {
                        return Ok(RelationProjectionMutation::AlreadyApplied);
                    }
                    return Err(GraphRepositoryError::ConstraintViolation(
                        "equal relation projection fence is bound to a different mutation"
                            .to_string(),
                    ));
                }
            }

            let changed = match current {
                None => {
                    sqlx::query(
                        r"INSERT INTO relation_edges
                          (src_table, src_id, relation, tgt_table, tgt_id, metadata)
                          VALUES (?, ?, ?, ?, ?, ?)
                          ON CONFLICT (src_table, src_id, relation, tgt_table, tgt_id) DO NOTHING",
                    )
                    .bind(source_table)
                    .bind(source_id)
                    .bind(&tombstone_relation)
                    .bind(target_table)
                    .bind(target_id)
                    .bind(&tombstone_metadata)
                    .execute(&mut *transaction)
                    .await
                    .map_err(Self::gerr)?
                    .rows_affected()
                        == 1
                }
                Some((stored_relation, raw)) if stored_relation == &tombstone_relation => {
                    sqlx::query(
                        r"UPDATE relation_edges SET metadata = ?
                          WHERE src_table = ? AND src_id = ? AND relation = ?
                            AND tgt_table = ? AND tgt_id = ? AND metadata IS ?",
                    )
                    .bind(&tombstone_metadata)
                    .bind(source_table)
                    .bind(source_id)
                    .bind(&tombstone_relation)
                    .bind(target_table)
                    .bind(target_id)
                    .bind(raw)
                    .execute(&mut *transaction)
                    .await
                    .map_err(Self::gerr)?
                    .rows_affected()
                        == 1
                }
                Some((_, raw)) => {
                    let deleted = sqlx::query(
                        r"DELETE FROM relation_edges
                          WHERE src_table = ? AND src_id = ? AND relation = ?
                            AND tgt_table = ? AND tgt_id = ? AND metadata IS ?",
                    )
                    .bind(source_table)
                    .bind(source_id)
                    .bind(relation)
                    .bind(target_table)
                    .bind(target_id)
                    .bind(raw)
                    .execute(&mut *transaction)
                    .await
                    .map_err(Self::gerr)?
                    .rows_affected()
                        == 1;
                    if !deleted {
                        false
                    } else {
                        sqlx::query(
                            r"INSERT INTO relation_edges
                              (src_table, src_id, relation, tgt_table, tgt_id, metadata)
                              VALUES (?, ?, ?, ?, ?, ?)
                              ON CONFLICT (src_table, src_id, relation, tgt_table, tgt_id) DO NOTHING",
                        )
                        .bind(source_table)
                        .bind(source_id)
                        .bind(&tombstone_relation)
                        .bind(target_table)
                        .bind(target_id)
                        .bind(&tombstone_metadata)
                        .execute(&mut *transaction)
                        .await
                        .map_err(Self::gerr)?
                        .rows_affected()
                            == 1
                    }
                }
            };
            if changed {
                transaction.commit().await.map_err(Self::gerr)?;
                return Ok(RelationProjectionMutation::Applied);
            }
            transaction.rollback().await.map_err(Self::gerr)?;
        }
        Err(GraphRepositoryError::Storage(
            "relation projection delete exhausted conditional retries".to_string(),
        ))
    }

    async fn unrelate_all(
        &self,
        table: &str,
        id: &str,
        relation_names: &[&str],
    ) -> Result<(), GraphRepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(Self::gerr)?;
        Self::ensure_legacy_entity_unfenced(&mut transaction, table, id, relation_names).await?;
        let mut sql = String::from(
            r"DELETE FROM relation_edges
              WHERE ((src_table = ? AND src_id = ?) OR (tgt_table = ? AND tgt_id = ?))",
        );
        if !relation_names.is_empty() {
            let _ = write!(
                sql,
                " AND relation IN ({})",
                Self::placeholders(relation_names.len())
            );
        }
        let mut q = sqlx::query(&sql).bind(table).bind(id).bind(table).bind(id);
        for rel in relation_names {
            q = q.bind(*rel);
        }
        q.execute(&mut *transaction).await.map_err(Self::gerr)?;
        transaction.commit().await.map_err(Self::gerr)?;
        Ok(())
    }

    /// Full bidirectional ghost-edge sweep. Runs as a loop of bounded deletes
    /// (`SELECT rowid … LIMIT 512` + `DELETE … WHERE rowid IN (…)`) so a hub
    /// entity never pins the writer lock for one giant transaction.
    async fn sweep_entity(&self, table: &str, id: &str) -> Result<u64, GraphRepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(Self::gerr)?;
        Self::ensure_legacy_entity_unfenced(&mut transaction, table, id, &[]).await?;
        let mut swept: u64 = 0;
        loop {
            let rowids: Vec<(i64,)> = sqlx::query_as(
                r"SELECT rowid FROM relation_edges
                  WHERE (src_table = ? AND src_id = ?) OR (tgt_table = ? AND tgt_id = ?)
                  LIMIT ?",
            )
            .bind(table)
            .bind(id)
            .bind(table)
            .bind(id)
            .bind(SWEEP_CHUNK as i64)
            .fetch_all(&mut *transaction)
            .await
            .map_err(Self::gerr)?;
            if rowids.is_empty() {
                break;
            }
            let sql = format!(
                "DELETE FROM relation_edges WHERE rowid IN ({})",
                Self::placeholders(rowids.len())
            );
            let mut q = sqlx::query(&sql);
            for (rid,) in &rowids {
                q = q.bind(*rid);
            }
            let res = q.execute(&mut *transaction).await.map_err(Self::gerr)?;
            swept += res.rows_affected();
            if rowids.len() < SWEEP_CHUNK {
                break;
            }
        }
        transaction.commit().await.map_err(Self::gerr)?;
        Ok(swept)
    }

    async fn exists(
        &self,
        source_table: &str,
        source_id: &str,
        target_table: &str,
        target_id: &str,
        relation: &str,
    ) -> Result<bool, GraphRepositoryError> {
        let row = sqlx::query(
            r"SELECT 1 FROM relation_edges
              WHERE src_table = ? AND src_id = ? AND relation = ?
                AND tgt_table = ? AND tgt_id = ?
              LIMIT 1",
        )
        .bind(source_table)
        .bind(source_id)
        .bind(relation)
        .bind(target_table)
        .bind(target_id)
        .fetch_optional(&*self.pool)
        .await
        .map_err(Self::gerr)?;
        Ok(row.is_some())
    }

    // ── Query Operations ─────────────────────────────────────

    async fn find_targets(
        &self,
        source_table: &str,
        source_id: &str,
        relation: &str,
    ) -> Result<Vec<(String, String)>, GraphRepositoryError> {
        let sql = format!(
            "SELECT tgt_table, tgt_id FROM relation_edges
             WHERE src_table = ? AND src_id = ? AND relation = ? {EDGE_ORDER}"
        );
        self.fetch_pairs(&sql, &[source_table, source_id, relation], None)
            .await
    }

    async fn find_sources(
        &self,
        target_table: &str,
        target_id: &str,
        relation: &str,
    ) -> Result<Vec<(String, String)>, GraphRepositoryError> {
        let sql = format!(
            "SELECT src_table, src_id FROM relation_edges
             WHERE tgt_table = ? AND tgt_id = ? AND relation = ? {EDGE_ORDER}"
        );
        self.fetch_pairs(&sql, &[target_table, target_id, relation], None)
            .await
    }

    async fn find_targets_by_table(
        &self,
        source_table: &str,
        source_id: &str,
        relation: &str,
        target_table_filter: &str,
    ) -> Result<Vec<(String, String)>, GraphRepositoryError> {
        // KV semantics: an empty filter means "no filtering".
        if target_table_filter.is_empty() {
            return self.find_targets(source_table, source_id, relation).await;
        }
        let sql = format!(
            "SELECT tgt_table, tgt_id FROM relation_edges
             WHERE src_table = ? AND src_id = ? AND relation = ? AND tgt_table = ? {EDGE_ORDER}"
        );
        self.fetch_pairs(
            &sql,
            &[source_table, source_id, relation, target_table_filter],
            None,
        )
        .await
    }

    async fn find_sources_by_table(
        &self,
        target_table: &str,
        target_id: &str,
        relation: &str,
        source_table_filter: &str,
    ) -> Result<Vec<(String, String)>, GraphRepositoryError> {
        if source_table_filter.is_empty() {
            return self.find_sources(target_table, target_id, relation).await;
        }
        let sql = format!(
            "SELECT src_table, src_id FROM relation_edges
             WHERE tgt_table = ? AND tgt_id = ? AND relation = ? AND src_table = ? {EDGE_ORDER}"
        );
        self.fetch_pairs(
            &sql,
            &[target_table, target_id, relation, source_table_filter],
            None,
        )
        .await
    }

    async fn find_targets_paginated(
        &self,
        source_table: &str,
        source_id: &str,
        relation: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(String, String)>, GraphRepositoryError> {
        let sql = format!(
            "SELECT tgt_table, tgt_id FROM relation_edges
             WHERE src_table = ? AND src_id = ? AND relation = ? {EDGE_ORDER} LIMIT ? OFFSET ?"
        );
        self.fetch_pairs(
            &sql,
            &[source_table, source_id, relation],
            Some((limit, offset)),
        )
        .await
    }

    async fn find_sources_paginated(
        &self,
        target_table: &str,
        target_id: &str,
        relation: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(String, String)>, GraphRepositoryError> {
        let sql = format!(
            "SELECT src_table, src_id FROM relation_edges
             WHERE tgt_table = ? AND tgt_id = ? AND relation = ? {EDGE_ORDER} LIMIT ? OFFSET ?"
        );
        self.fetch_pairs(
            &sql,
            &[target_table, target_id, relation],
            Some((limit, offset)),
        )
        .await
    }

    async fn find_targets_by_table_paginated(
        &self,
        source_table: &str,
        source_id: &str,
        relation: &str,
        target_table_filter: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(String, String)>, GraphRepositoryError> {
        if target_table_filter.is_empty() {
            return self
                .find_targets_paginated(source_table, source_id, relation, limit, offset)
                .await;
        }
        let sql = format!(
            "SELECT tgt_table, tgt_id FROM relation_edges
             WHERE src_table = ? AND src_id = ? AND relation = ? AND tgt_table = ?
             {EDGE_ORDER} LIMIT ? OFFSET ?"
        );
        self.fetch_pairs(
            &sql,
            &[source_table, source_id, relation, target_table_filter],
            Some((limit, offset)),
        )
        .await
    }

    async fn find_sources_by_table_paginated(
        &self,
        target_table: &str,
        target_id: &str,
        relation: &str,
        source_table_filter: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(String, String)>, GraphRepositoryError> {
        if source_table_filter.is_empty() {
            return self
                .find_sources_paginated(target_table, target_id, relation, limit, offset)
                .await;
        }
        let sql = format!(
            "SELECT src_table, src_id FROM relation_edges
             WHERE tgt_table = ? AND tgt_id = ? AND relation = ? AND src_table = ?
             {EDGE_ORDER} LIMIT ? OFFSET ?"
        );
        self.fetch_pairs(
            &sql,
            &[target_table, target_id, relation, source_table_filter],
            Some((limit, offset)),
        )
        .await
    }

    async fn find_related(
        &self,
        table: &str,
        id: &str,
        relation_names: &[&str],
    ) -> Result<Vec<(String, String)>, GraphRepositoryError> {
        // Mirrors the KV adapter: per relation, targets then sources,
        // first-seen dedup. An empty relation list yields an empty result.
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        for &rel in relation_names {
            for (t, i) in self.find_targets(table, id, rel).await? {
                if seen.insert((t.clone(), i.clone())) {
                    result.push((t, i));
                }
            }
            for (t, i) in self.find_sources(table, id, rel).await? {
                if seen.insert((t.clone(), i.clone())) {
                    result.push((t, i));
                }
            }
        }
        Ok(result)
    }

    async fn find_all_relations(
        &self,
        table: &str,
        id: &str,
        relation_names: &[&str],
    ) -> Result<HashMap<String, Vec<(String, String)>>, GraphRepositoryError> {
        // Mirrors the KV adapter: outgoing targets only, one entry per
        // requested relation (present even when empty).
        let mut map: HashMap<String, Vec<(String, String)>> = HashMap::new();
        for &rel in relation_names {
            let targets = self.find_targets(table, id, rel).await?;
            map.entry(rel.to_string()).or_default().extend(targets);
        }
        Ok(map)
    }

    async fn find_by_type(
        &self,
        relation: &str,
    ) -> Result<Vec<((String, String), (String, String))>, GraphRepositoryError> {
        let sql = format!(
            "SELECT src_table, src_id, tgt_table, tgt_id FROM relation_edges
             WHERE relation = ? {EDGE_ORDER}"
        );
        let rows: Vec<(String, String, String, String)> = sqlx::query_as(&sql)
            .bind(relation)
            .fetch_all(&*self.pool)
            .await
            .map_err(Self::gerr)?;
        Ok(rows
            .into_iter()
            .map(|(st, si, tt, ti)| ((st, si), (tt, ti)))
            .collect())
    }

    async fn find_by_types(
        &self,
        relation_names: &[&str],
    ) -> Result<Vec<((String, String), (String, String), String)>, GraphRepositoryError> {
        if relation_names.is_empty() {
            return Ok(Vec::new());
        }
        let sql = format!(
            "SELECT src_table, src_id, tgt_table, tgt_id, relation FROM relation_edges
             WHERE relation IN ({}) {EDGE_ORDER}",
            Self::placeholders(relation_names.len())
        );
        let mut q = sqlx::query_as::<_, (String, String, String, String, String)>(&sql);
        for rel in relation_names {
            q = q.bind(*rel);
        }
        let rows = q.fetch_all(&*self.pool).await.map_err(Self::gerr)?;
        Ok(rows
            .into_iter()
            .map(|(st, si, tt, ti, rel)| ((st, si), (tt, ti), rel))
            .collect())
    }

    async fn find_targets_with_metadata(
        &self,
        source_table: &str,
        source_id: &str,
        relation: &str,
    ) -> Result<Vec<((String, String), Option<RelationMetadata>)>, GraphRepositoryError> {
        let sql = format!(
            "SELECT tgt_table, tgt_id, metadata FROM relation_edges
             WHERE src_table = ? AND src_id = ? AND relation = ? {EDGE_ORDER}"
        );
        let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(&sql)
            .bind(source_table)
            .bind(source_id)
            .bind(relation)
            .fetch_all(&*self.pool)
            .await
            .map_err(Self::gerr)?;
        Ok(rows
            .into_iter()
            .map(|(tt, ti, meta)| ((tt, ti), Self::parse_metadata(meta)))
            .collect())
    }

    async fn find_sources_with_metadata(
        &self,
        target_table: &str,
        target_id: &str,
        relation: &str,
    ) -> Result<Vec<((String, String), Option<RelationMetadata>)>, GraphRepositoryError> {
        let sql = format!(
            "SELECT src_table, src_id, metadata FROM relation_edges
             WHERE tgt_table = ? AND tgt_id = ? AND relation = ? {EDGE_ORDER}"
        );
        let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(&sql)
            .bind(target_table)
            .bind(target_id)
            .bind(relation)
            .fetch_all(&*self.pool)
            .await
            .map_err(Self::gerr)?;
        Ok(rows
            .into_iter()
            .map(|(st, si, meta)| ((st, si), Self::parse_metadata(meta)))
            .collect())
    }

    async fn get_stats(
        &self,
        relation_names: &[&str],
    ) -> Result<GraphRelationStats, GraphRepositoryError> {
        let mut sql = String::from(
            "SELECT relation, src_table, tgt_table, COUNT(*) AS c FROM relation_edges",
        );
        if relation_names.is_empty() {
            sql.push_str(" WHERE relation NOT LIKE ?");
        } else {
            let _ = write!(
                sql,
                " WHERE relation IN ({})",
                Self::placeholders(relation_names.len())
            );
        }
        sql.push_str(" GROUP BY relation, src_table, tgt_table");
        let mut q = sqlx::query(&sql);
        if relation_names.is_empty() {
            q = q.bind(format!("{PROJECTION_TOMBSTONE_PREFIX}%"));
        } else {
            for rel in relation_names {
                q = q.bind(*rel);
            }
        }
        let rows = q.fetch_all(&*self.pool).await.map_err(Self::gerr)?;

        let mut stats = GraphRelationStats::default();
        for row in rows {
            let rel: String = row.try_get("relation").map_err(Self::gerr)?;
            let st: String = row.try_get("src_table").map_err(Self::gerr)?;
            let tt: String = row.try_get("tgt_table").map_err(Self::gerr)?;
            let c: i64 = row.try_get("c").map_err(Self::gerr)?;
            let c = c as usize;
            *stats.by_type.entry(rel).or_default() += c;
            *stats.by_source_table.entry(st).or_default() += c;
            *stats.by_target_table.entry(tt).or_default() += c;
            stats.total += c;
        }
        Ok(stats)
    }

    // ── Batch Operations ─────────────────────────────────────

    async fn relate_batch(
        &self,
        relations: Vec<(String, String, String, String, String)>,
    ) -> Result<usize, GraphRepositoryError> {
        let mut tx = self.pool.begin().await.map_err(Self::gerr)?;
        for (st, si, tt, ti, rel) in &relations {
            Self::ensure_legacy_edge_unfenced(&mut tx, st, si, tt, ti, rel).await?;
        }
        let mut created = 0usize;
        for (st, si, tt, ti, rel) in &relations {
            let res = sqlx::query(
                r"INSERT INTO relation_edges (src_table, src_id, relation, tgt_table, tgt_id, metadata)
                  VALUES (?, ?, ?, ?, ?, NULL)
                  ON CONFLICT (src_table, src_id, relation, tgt_table, tgt_id) DO NOTHING",
            )
            .bind(st)
            .bind(si)
            .bind(rel)
            .bind(tt)
            .bind(ti)
            .execute(&mut *tx)
            .await
            .map_err(Self::gerr)?;
            created += res.rows_affected() as usize;
        }
        tx.commit().await.map_err(Self::gerr)?;
        Ok(created)
    }

    async fn unrelate_batch(
        &self,
        relations: Vec<(String, String, String, String, String)>,
    ) -> Result<usize, GraphRepositoryError> {
        let mut tx = self.pool.begin().await.map_err(Self::gerr)?;
        for (st, si, tt, ti, rel) in &relations {
            Self::ensure_legacy_edge_unfenced(&mut tx, st, si, tt, ti, rel).await?;
        }
        let mut deleted = 0usize;
        for (st, si, tt, ti, rel) in &relations {
            let res = sqlx::query(
                r"DELETE FROM relation_edges
                  WHERE src_table = ? AND src_id = ? AND relation = ?
                    AND tgt_table = ? AND tgt_id = ?",
            )
            .bind(st)
            .bind(si)
            .bind(rel)
            .bind(tt)
            .bind(ti)
            .execute(&mut *tx)
            .await
            .map_err(Self::gerr)?;
            deleted += res.rows_affected() as usize;
        }
        tx.commit().await.map_err(Self::gerr)?;
        Ok(deleted)
    }

    // ── Transactions (no-op — pool-based; each statement is atomic) ──

    async fn begin_transaction(&self) -> Result<(), GraphRepositoryError> {
        Ok(())
    }
    async fn commit_transaction(&self) -> Result<(), GraphRepositoryError> {
        Ok(())
    }
    async fn rollback_transaction(&self) -> Result<(), GraphRepositoryError> {
        Ok(())
    }

    // ── Entity Operations (read entity tables in the same file) ──

    async fn get_record_json(
        &self,
        table: &str,
        id: &str,
    ) -> Result<Option<serde_json::Value>, GraphRepositoryError> {
        if !crate::is_bare_ident(table) {
            return Ok(None);
        }
        // Accept both bare ids and "table:id" RecordId strings.
        let bare_id = id.strip_prefix(&format!("{table}:")).unwrap_or(id);
        let sql =
            format!("SELECT id, data, version FROM {table} WHERE id = ? AND deleted_at IS NULL");
        let row = match sqlx::query(&sql)
            .bind(bare_id)
            .fetch_optional(&*self.pool)
            .await
        {
            Ok(row) => row,
            Err(e) if Self::is_missing_table(&e) => return Ok(None),
            Err(e) => return Err(Self::gerr(e)),
        };
        let Some(row) = row else { return Ok(None) };
        let row_id: String = row.try_get("id").map_err(Self::gerr)?;
        let data: String = row.try_get("data").map_err(Self::gerr)?;
        let version: i64 = row.try_get("version").map_err(Self::gerr)?;
        let mut value: serde_json::Value = serde_json::from_str(&data)
            .map_err(|e| GraphRepositoryError::Storage(format!("parse entity JSON: {e}")))?;
        if let serde_json::Value::Object(map) = &mut value {
            // Same materialization as SqliteRepository: RecordId string + version.
            map.insert(
                "id".to_string(),
                serde_json::Value::String(format!("{table}:{row_id}")),
            );
            map.insert("version".to_string(), serde_json::Value::from(version));
        }
        Ok(Some(value))
    }

    async fn set_namespace(
        &self,
        _namespace: &str,
        _database: &str,
    ) -> Result<(), GraphRepositoryError> {
        // No-op: SQLite has no namespace concept; isolation is per-file.
        Ok(())
    }

    async fn list_record_ids(
        &self,
        table: &str,
    ) -> Result<Vec<(String, String)>, GraphRepositoryError> {
        if !crate::is_bare_ident(table) {
            return Ok(Vec::new());
        }
        let sql = format!("SELECT id FROM {table} WHERE deleted_at IS NULL ORDER BY id");
        let rows: Vec<(String,)> = match sqlx::query_as(&sql).fetch_all(&*self.pool).await {
            Ok(rows) => rows,
            Err(e) if Self::is_missing_table(&e) => return Ok(Vec::new()),
            Err(e) => return Err(Self::gerr(e)),
        };
        Ok(rows
            .into_iter()
            .map(|(id,)| (table.to_string(), id))
            .collect())
    }

    async fn query_records_by_field(
        &self,
        table: &str,
        field_path: &str,
        value: &serde_json::Value,
    ) -> Result<Vec<serde_json::Value>, GraphRepositoryError> {
        self.query_records_by_conditions(table, &[(field_path, value.clone())])
            .await
    }

    async fn query_records_by_conditions(
        &self,
        table: &str,
        conditions: &[(&str, serde_json::Value)],
    ) -> Result<Vec<serde_json::Value>, GraphRepositoryError> {
        if !crate::is_bare_ident(table) {
            return Err(GraphRepositoryError::InvalidQuery(format!(
                "rejected unsafe table identifier: {table:?}"
            )));
        }
        let mut sql = format!("SELECT id, data, version FROM {table} WHERE deleted_at IS NULL");
        let mut binds: Vec<String> = Vec::with_capacity(conditions.len());
        for (field_path, value) in conditions {
            if !field_path.split('.').all(crate::is_valid_path_segment) {
                return Err(GraphRepositoryError::InvalidQuery(format!(
                    "rejected unsafe field path: {field_path:?}"
                )));
            }
            // Both sides through json_extract so scalars compare natively
            // (same approach as the crate's build_json_where helper).
            let _ = write!(
                sql,
                " AND json_extract(data, '$.{field_path}') = json_extract(?, '$')"
            );
            binds.push(serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()));
        }
        sql.push_str(" ORDER BY id");
        let mut q = sqlx::query(&sql);
        for b in &binds {
            q = q.bind(b.as_str());
        }
        let rows = match q.fetch_all(&*self.pool).await {
            Ok(rows) => rows,
            Err(e) if Self::is_missing_table(&e) => return Ok(Vec::new()),
            Err(e) => return Err(Self::gerr(e)),
        };
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let row_id: String = row.try_get("id").map_err(Self::gerr)?;
            let data: String = row.try_get("data").map_err(Self::gerr)?;
            let version: i64 = row.try_get("version").map_err(Self::gerr)?;
            let mut value: serde_json::Value = serde_json::from_str(&data)
                .map_err(|e| GraphRepositoryError::Storage(format!("parse entity JSON: {e}")))?;
            if let serde_json::Value::Object(map) = &mut value {
                map.insert(
                    "id".to_string(),
                    serde_json::Value::String(format!("{table}:{row_id}")),
                );
                map.insert("version".to_string(), serde_json::Value::from(version));
            }
            out.push(value);
        }
        Ok(out)
    }
}
