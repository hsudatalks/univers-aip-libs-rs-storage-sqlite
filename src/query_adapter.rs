//! Query Adapter for SQLite
//!
//! Renders a `QueryBuilder` AST directly into SQLite-flavored SQL.
//!
//! Entity fields live inside the JSON `data` column and are accessed via
//! `json_extract(data, '$.field')`. Storage metadata (`id`, `created_at`,
//! `updated_at`, `deleted_at`, `version`) are real columns and referenced
//! directly. Every user-supplied value is bound as a `?` parameter — no
//! interpolation — so the output is SQL-injection-safe by construction.

use std::fmt::Write;

use univers_aip_contracts_data::storage::query_builder::sanitize_field_name;
use univers_aip_contracts_data::storage::{
    LogicalOperator, QueryBuilder, QueryCondition, QueryOperator, QueryValue, RepositoryError,
    RepositoryResult, SortDirection,
};

/// A rendered SQLite query: SQL text with `?` placeholders plus the ordered
/// parameter values to bind (index corresponds to placeholder order).
#[derive(Debug)]
pub struct SqliteQuery {
    pub sql: String,
    pub params: Vec<SqliteParam>,
}

/// A parameter value to bind into a SQLite statement.
#[derive(Debug, Clone)]
pub enum SqliteParam {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
}

/// SQLite query adapter — renders `QueryBuilder` AST into SQLite SQL.
pub struct SqliteQueryAdapter;

/// Real table columns (not stored inside the JSON `data` blob).
const METADATA_COLUMNS: &[&str] = &[
    "id",
    "data",
    "created_at",
    "updated_at",
    "deleted_at",
    "version",
];

impl SqliteQueryAdapter {
    /// Columns every read returns (storage-level id/data).
    /// `version`, `created_at`/`updated_at`/`deleted_at` are write/index-only
    /// and not projected back — the entity reconstructs its own version +
    /// timestamps from `data` (matching SurrealDB, where `version` is part of
    /// the entity content, not a storage side-channel).
    const SELECT_COLUMNS: &'static str = "id, data";

    fn is_metadata_column(field: &str) -> bool {
        METADATA_COLUMNS.contains(&field)
    }

    /// The SQL expression for a field: bare column for metadata, else a
    /// `json_extract` into the `data` blob. `field` must already be sanitized.
    fn field_expr(field: &str) -> String {
        if Self::is_metadata_column(field) {
            field.to_string()
        } else if Self::is_encoded_metadata_field(field) {
            Self::encoded_metadata_field_expr(field)
        } else {
            format!("json_extract(data, '$.{field}')")
        }
    }

    /// Some legacy entity fields are stored as JSON-encoded strings inside
    /// the entity blob. The explicit `encoded_metadata.*` query namespace unwraps that
    /// string before reading a nested key, while retaining ordinary nested
    /// JSON semantics for every other entity field.
    fn is_encoded_metadata_field(field: &str) -> bool {
        field.starts_with("encoded_metadata.")
    }

    fn encoded_metadata_field_expr(field: &str) -> String {
        let path = field.strip_prefix("encoded_metadata.").unwrap_or_default();
        let metadata = "json_extract(data, '$.metadata')";
        let parsed = format!("CASE WHEN json_valid({metadata}) THEN {metadata} ELSE NULL END");
        // The field path is exact. Missing metadata or keys remain SQL NULL;
        // callers own any alias or legacy tenant migration policy.
        format!("json_extract({parsed}, '$.{path}')")
    }

    /// Sanitize a table/column identifier (alphanumeric + underscore only).
    fn sanitize_identifier(name: &str) -> String {
        name.chars()
            .filter(|c| c.is_alphanumeric() || *c == '_')
            .collect()
    }

    // ------------------------------------------------------------------
    // DDL
    // ------------------------------------------------------------------

    /// `CREATE TABLE IF NOT EXISTS` for the standard entity schema.
    /// Idempotent — safe to run on every operation.
    ///
    /// There is **no `version` column**: the entity's `version` field lives
    /// inside the JSON `data` (SurrealDB stores it the same way), so it
    /// round-trips with whatever type the entity uses (u64, String, …) and
    /// optimistic locking reads it via `json_extract(data,'$.version')`.
    pub fn build_create_table(table: &str) -> String {
        let t = Self::sanitize_identifier(table);
        format!(
            r"CREATE TABLE IF NOT EXISTS {t} (
    id         TEXT PRIMARY KEY,
    data       TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    deleted_at TEXT
)"
        )
    }

    // ------------------------------------------------------------------
    // Statement builders (string-only; no user params except the id/timestamps
    // bound by the repository)
    // ------------------------------------------------------------------

    pub fn build_select_by_id(table: &str, soft_delete: bool) -> String {
        let t = Self::sanitize_identifier(table);
        if soft_delete {
            format!(
                "SELECT {} FROM {t} WHERE id = ? AND deleted_at IS NULL",
                Self::SELECT_COLUMNS
            )
        } else {
            format!("SELECT {} FROM {t} WHERE id = ?", Self::SELECT_COLUMNS)
        }
    }

    /// INSERT with idempotent `ON CONFLICT DO NOTHING`. On a duplicate id the
    /// statement inserts no row and `RETURNING` yields nothing — the caller
    /// interprets that as `Ok(None)` (safe to retry).
    pub fn build_insert(table: &str) -> String {
        let t = Self::sanitize_identifier(table);
        format!(
            "INSERT INTO {t} (id, data, created_at, updated_at) \
             VALUES (?, ?, ?, ?) \
             ON CONFLICT(id) DO NOTHING \
             RETURNING {}",
            Self::SELECT_COLUMNS
        )
    }

    pub fn build_update(table: &str) -> String {
        let t = Self::sanitize_identifier(table);
        format!(
            "UPDATE {t} SET data = ?, updated_at = ? \
             WHERE id = ? AND deleted_at IS NULL RETURNING {}",
            Self::SELECT_COLUMNS
        )
    }

    /// Optimistic-lock UPDATE — only fires when the stored `version` (read
    /// from the entity's JSON `data`) matches `expected_version`. The entity
    /// owns its `version` field, so no storage-side auto-increment (matches
    /// SurrealDB, where `UPDATE … WHERE version = $version` checks the record
    /// content).
    pub fn build_update_with_version(table: &str) -> String {
        let t = Self::sanitize_identifier(table);
        format!(
            "UPDATE {t} SET data = ?, updated_at = ? \
             WHERE id = ? AND deleted_at IS NULL AND COALESCE(json_extract(data, '$.record_version'), \
             json_extract(data, '$.entity_version'), json_extract(data, '$.version'), 0) = ? RETURNING {}",
            Self::SELECT_COLUMNS
        )
    }

    /// Optimistic-lock UPDATE additionally guarded by a JSON timestamp.
    /// SQLite's `julianday` parses the timestamp into an instant, so offsets
    /// are compared correctly and malformed values yield NULL (fail closed).
    /// `'now'` is SQLite's stable time for the executing `sqlite3_step`, so a
    /// caller cannot supply a stale pre-mutation deadline.
    pub fn build_update_with_version_and_timestamp_guard(
        table: &str,
        timestamp_field: &str,
    ) -> RepositoryResult<String> {
        let field = Self::validated_json_field(timestamp_field)?;
        let t = Self::sanitize_identifier(table);
        let json_type = format!("json_type(data, '$.{field}')");
        let json_value = format!("json_extract(data, '$.{field}')");
        let rfc3339_shape = Self::rfc3339_shape_predicate(&json_value);
        Ok(format!(
            "UPDATE {t} SET data = ?, updated_at = ? \
             WHERE id = ? AND deleted_at IS NULL AND COALESCE(json_extract(data, '$.record_version'), \
             json_extract(data, '$.entity_version'), json_extract(data, '$.version'), 0) = ? \
             AND ({json_type} IS NULL OR {json_type} = 'null' OR \
             ({json_type} = 'text' AND {rfc3339_shape} AND julianday({json_value}) IS NOT NULL \
             AND julianday({json_value}) > julianday('now'))) RETURNING {}",
            Self::SELECT_COLUMNS
        ))
    }

    /// Optimistic-lock UPDATE that fires only once a valid stored JSON
    /// timestamp has expired at SQLite's mutation-time clock.
    pub fn build_update_with_version_and_expired_timestamp_guard(
        table: &str,
        timestamp_field: &str,
    ) -> RepositoryResult<String> {
        let field = Self::validated_json_field(timestamp_field)?;
        let t = Self::sanitize_identifier(table);
        let json_type = format!("json_type(data, '$.{field}')");
        let json_value = format!("json_extract(data, '$.{field}')");
        let rfc3339_shape = Self::rfc3339_shape_predicate(&json_value);
        Ok(format!(
            "UPDATE {t} SET data = ?, updated_at = ? \
             WHERE id = ? AND deleted_at IS NULL AND COALESCE(json_extract(data, '$.record_version'), \
             json_extract(data, '$.entity_version'), json_extract(data, '$.version'), 0) = ? \
             AND {json_type} = 'text' AND {rfc3339_shape} \
             AND julianday({json_value}) IS NOT NULL \
             AND julianday({json_value}) <= julianday('now') RETURNING {}",
            Self::SELECT_COLUMNS
        ))
    }

    fn validated_json_field(field: &str) -> RepositoryResult<String> {
        let sanitized = sanitize_field_name(field);
        if sanitized.is_empty() || sanitized != field || field.split('.').any(str::is_empty) {
            return Err(RepositoryError::validation(format!(
                "invalid JSON timestamp field '{field}'"
            )));
        }
        Ok(sanitized)
    }

    fn rfc3339_shape_predicate(value: &str) -> String {
        let date_time = format!(
            "substr({value}, 1, 19) GLOB \
             '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]'"
        );
        let z_suffix = format!(
            "((length({value}) = 20 OR (length({value}) > 21 \
             AND substr({value}, 20, 1) = '.' \
             AND substr({value}, 21, length({value}) - 21) NOT GLOB '*[^0-9]*')) \
             AND substr({value}, -1) = 'Z')"
        );
        let offset_suffix = format!(
            "((length({value}) = 25 OR (length({value}) > 26 \
             AND substr({value}, 20, 1) = '.' \
             AND substr({value}, 21, length({value}) - 26) NOT GLOB '*[^0-9]*')) \
             AND substr({value}, -6, 1) IN ('+', '-') \
             AND substr({value}, -5) GLOB '[0-9][0-9]:[0-9][0-9]')"
        );
        format!("({date_time} AND ({z_suffix} OR {offset_suffix}))")
    }

    /// Hard delete — physically removes the row.
    pub fn build_delete(table: &str) -> String {
        let t = Self::sanitize_identifier(table);
        format!(
            "DELETE FROM {t} WHERE id = ? RETURNING {}",
            Self::SELECT_COLUMNS
        )
    }

    /// Hard delete guarded by the entity version stored in JSON data.
    pub fn build_delete_with_version(table: &str) -> String {
        let t = Self::sanitize_identifier(table);
        format!(
            "DELETE FROM {t} WHERE id = ? AND \
             COALESCE(json_extract(data, '$.record_version'), \
             json_extract(data, '$.entity_version'), json_extract(data, '$.version'), 0) = ? RETURNING {}",
            Self::SELECT_COLUMNS
        )
    }

    /// Soft delete — marks `deleted_at`; reads filter it out via `IS NULL`.
    pub fn build_soft_delete(table: &str) -> String {
        let t = Self::sanitize_identifier(table);
        format!(
            "UPDATE {t} SET deleted_at = ? WHERE id = ? AND deleted_at IS NULL RETURNING {}",
            Self::SELECT_COLUMNS
        )
    }

    /// Soft delete guarded by the entity version stored in JSON data.
    pub fn build_soft_delete_with_version(table: &str) -> String {
        let t = Self::sanitize_identifier(table);
        format!(
            "UPDATE {t} SET deleted_at = ? WHERE id = ? AND deleted_at IS NULL \
             AND COALESCE(json_extract(data, '$.record_version'), \
             json_extract(data, '$.entity_version'), json_extract(data, '$.version'), 0) = ? RETURNING {}",
            Self::SELECT_COLUMNS
        )
    }

    pub fn build_select_all(table: &str, soft_delete: bool) -> String {
        let t = Self::sanitize_identifier(table);
        if soft_delete {
            format!(
                "SELECT {} FROM {t} WHERE deleted_at IS NULL",
                Self::SELECT_COLUMNS
            )
        } else {
            format!("SELECT {} FROM {t}", Self::SELECT_COLUMNS)
        }
    }

    pub fn build_count(table: &str, soft_delete: bool) -> String {
        let t = Self::sanitize_identifier(table);
        if soft_delete {
            format!("SELECT COUNT(*) AS count FROM {t} WHERE deleted_at IS NULL")
        } else {
            format!("SELECT COUNT(*) AS count FROM {t}")
        }
    }

    pub fn build_select_in(table: &str, n_ids: usize, soft_delete: bool) -> String {
        let t = Self::sanitize_identifier(table);
        let placeholders = vec!["?"; n_ids].join(", ");
        if soft_delete {
            format!(
                "SELECT {} FROM {t} WHERE id IN ({placeholders}) AND deleted_at IS NULL",
                Self::SELECT_COLUMNS
            )
        } else {
            format!(
                "SELECT {} FROM {t} WHERE id IN ({placeholders})",
                Self::SELECT_COLUMNS
            )
        }
    }

    // ------------------------------------------------------------------
    // QueryBuilder → parameterized SELECT / COUNT
    // ------------------------------------------------------------------

    /// Build a `SELECT … FROM <table>` honoring the QueryBuilder's conditions,
    /// ordering, limit and offset. `soft_delete` additionally filters out
    /// soft-deleted rows.
    pub fn build_select(table: &str, builder: &QueryBuilder, soft_delete: bool) -> SqliteQuery {
        let t = Self::sanitize_identifier(table);
        let mut params = Vec::new();
        let mut sql = format!("SELECT {} FROM {t}", Self::SELECT_COLUMNS);
        sql.push_str(&Self::assemble_where(builder, soft_delete, &mut params));

        if let Some(order) = builder.order_by_clause() {
            let dir = match order.direction {
                SortDirection::Asc => "ASC",
                SortDirection::Desc => "DESC",
            };
            let _ = write!(sql, " ORDER BY {} {dir}", Self::field_expr(&order.field));
        }
        if let Some(limit) = builder.limit_value() {
            let _ = write!(sql, " LIMIT {limit}");
        }
        if let Some(offset) = builder.offset_value() {
            let _ = write!(sql, " OFFSET {offset}");
        }

        SqliteQuery { sql, params }
    }

    /// Build a `SELECT COUNT(*) …` honoring the QueryBuilder conditions.
    pub fn build_count_filtered(
        table: &str,
        builder: &QueryBuilder,
        soft_delete: bool,
    ) -> SqliteQuery {
        let t = Self::sanitize_identifier(table);
        let mut params = Vec::new();
        let mut sql = format!("SELECT COUNT(*) AS count FROM {t}");
        sql.push_str(&Self::assemble_where(builder, soft_delete, &mut params));
        SqliteQuery { sql, params }
    }

    /// Assemble the `WHERE` clause (with leading ` WHERE`) or an empty string.
    ///
    /// When both a soft-delete filter and user conditions are present, the user
    /// conditions are parenthesized so that inner `OR`s don't leak past the
    /// soft-delete guard (e.g. `deleted_at IS NULL AND (a = ? OR b = ?)`).
    fn assemble_where(
        builder: &QueryBuilder,
        soft_delete: bool,
        params: &mut Vec<SqliteParam>,
    ) -> String {
        let conditions = builder.conditions();
        let mut parts: Vec<String> = Vec::with_capacity(conditions.len() * 2);
        for (idx, cond) in conditions.iter().enumerate() {
            if idx > 0 {
                let join = match conditions[idx - 1].logical_op {
                    LogicalOperator::And => "AND",
                    LogicalOperator::Or => "OR",
                };
                parts.push(join.to_string());
            }
            parts.push(Self::render_condition(cond, params));
        }
        let conds_sql = parts.join(" ");
        let has_conds = !conds_sql.is_empty();

        match (soft_delete, has_conds) {
            (true, true) => format!(" WHERE deleted_at IS NULL AND ({conds_sql})"),
            (true, false) => " WHERE deleted_at IS NULL".to_string(),
            (false, true) => format!(" WHERE {conds_sql}"),
            (false, false) => String::new(),
        }
    }

    /// Render one AST condition into a SQL fragment, pushing bound params.
    fn render_condition(cond: &QueryCondition, params: &mut Vec<SqliteParam>) -> String {
        let field = sanitize_field_name(&cond.field);

        match cond.operator {
            QueryOperator::IsNull => format!("{} IS NULL", Self::field_expr(&field)),
            QueryOperator::IsNotNull => format!("{} IS NOT NULL", Self::field_expr(&field)),

            QueryOperator::In | QueryOperator::NotIn => {
                Self::render_in(&field, &cond.operator, &cond.value, params)
            }

            QueryOperator::Contains | QueryOperator::ContainsNot => {
                Self::render_contains(&field, &cond.operator, &cond.value, params)
            }
            QueryOperator::ContainsAll => Self::render_contains_all(&field, &cond.value, params),
            QueryOperator::ContainsAny => Self::render_contains_any(&field, &cond.value, params),

            // Simple binary comparison: <expr> <op> ?
            QueryOperator::Equals
            | QueryOperator::NotEquals
            | QueryOperator::GreaterThan
            | QueryOperator::GreaterThanOrEqual
            | QueryOperator::LessThan
            | QueryOperator::LessThanOrEqual
            | QueryOperator::Like => {
                params.push(Self::convert_query_value(&cond.value));
                let op = Self::binary_op(&cond.operator);
                format!("{} {op} ?", Self::field_expr(&field))
            }
        }
    }

    fn binary_op(op: &QueryOperator) -> &'static str {
        match op {
            QueryOperator::Equals => "=",
            QueryOperator::NotEquals => "<>",
            QueryOperator::GreaterThan => ">",
            QueryOperator::GreaterThanOrEqual => ">=",
            QueryOperator::LessThan => "<",
            QueryOperator::LessThanOrEqual => "<=",
            QueryOperator::Like => "LIKE",
            _ => unreachable!("binary_op only handles simple comparison operators"),
        }
    }

    /// `IN (?, …)` / `NOT IN (?, …)`. An empty value list degenerates to a
    /// constant predicate (`IN ()` is invalid SQL).
    fn render_in(
        field: &str,
        op: &QueryOperator,
        value: &QueryValue,
        params: &mut Vec<SqliteParam>,
    ) -> String {
        let expr = Self::field_expr(field);
        let arr = match value {
            QueryValue::Array(a) => a.clone(),
            other => vec![other.to_json_value()],
        };
        if arr.is_empty() {
            return match op {
                QueryOperator::NotIn => "1 = 1".to_string(),
                _ => "1 = 0".to_string(),
            };
        }
        let placeholders = vec!["?"; arr.len()].join(", ");
        for v in &arr {
            params.push(Self::json_value_to_param(v));
        }
        let kw = match op {
            QueryOperator::NotIn => "NOT IN",
            _ => "IN",
        };
        format!("{expr} {kw} ({placeholders})")
    }

    /// `field CONTAINS value` → `EXISTS (SELECT 1 FROM json_each(<field>) WHERE value = ?)`.
    fn render_contains(
        field: &str,
        op: &QueryOperator,
        value: &QueryValue,
        params: &mut Vec<SqliteParam>,
    ) -> String {
        let jx = format!("json_extract(data, '$.{field}')");
        let param = match value {
            QueryValue::Array(a) => a
                .first()
                .map(Self::json_value_to_param)
                .unwrap_or(SqliteParam::Null),
            other => Self::convert_query_value(other),
        };
        params.push(param);
        match op {
            QueryOperator::ContainsNot => {
                format!("NOT EXISTS (SELECT 1 FROM json_each({jx}) WHERE value = ?)")
            }
            _ => format!("EXISTS (SELECT 1 FROM json_each({jx}) WHERE value = ?)"),
        }
    }

    /// `field CONTAINSALL [v1..vN]` → distinct queried values present == N.
    fn render_contains_all(
        field: &str,
        value: &QueryValue,
        params: &mut Vec<SqliteParam>,
    ) -> String {
        let jx = format!("json_extract(data, '$.{field}')");
        let arr = match value {
            QueryValue::Array(a) => a.clone(),
            other => vec![other.to_json_value()],
        };
        let n = arr.len();
        if n == 0 {
            return "1 = 1".to_string();
        }
        let placeholders = vec!["?"; n].join(", ");
        for v in &arr {
            params.push(Self::json_value_to_param(v));
        }
        format!(
            "(SELECT COUNT(DISTINCT value) FROM json_each({jx}) WHERE value IN ({placeholders})) = {n}"
        )
    }

    /// `field CONTAINSANY [v1..vN]` → at least one queried value present.
    fn render_contains_any(
        field: &str,
        value: &QueryValue,
        params: &mut Vec<SqliteParam>,
    ) -> String {
        let jx = format!("json_extract(data, '$.{field}')");
        let arr = match value {
            QueryValue::Array(a) => a.clone(),
            other => vec![other.to_json_value()],
        };
        if arr.is_empty() {
            return "1 = 0".to_string();
        }
        let placeholders = vec!["?"; arr.len()].join(", ");
        for v in &arr {
            params.push(Self::json_value_to_param(v));
        }
        format!("EXISTS (SELECT 1 FROM json_each({jx}) WHERE value IN ({placeholders}))")
    }

    /// Convert a `QueryValue` (scalar) into a bind parameter.
    fn convert_query_value(value: &QueryValue) -> SqliteParam {
        match value {
            QueryValue::String(s) => SqliteParam::String(s.clone()),
            QueryValue::Number(n) => SqliteParam::Number(*n),
            QueryValue::Bool(b) => SqliteParam::Bool(*b),
            QueryValue::Null => SqliteParam::Null,
            QueryValue::Array(arr) => arr
                .first()
                .map(Self::json_value_to_param)
                .unwrap_or(SqliteParam::Null),
            QueryValue::DateTime(dt) => SqliteParam::String(dt.to_rfc3339()),
        }
    }

    /// Convert a `serde_json::Value` (array element) into a bind parameter.
    fn json_value_to_param(v: &serde_json::Value) -> SqliteParam {
        match v {
            serde_json::Value::String(s) => SqliteParam::String(s.clone()),
            serde_json::Value::Number(n) => SqliteParam::Number(n.as_f64().unwrap_or(0.0)),
            serde_json::Value::Bool(b) => SqliteParam::Bool(*b),
            serde_json::Value::Null => SqliteParam::Null,
            other => SqliteParam::String(other.to_string()),
        }
    }
}

/// Helper trait to fold a non-array `QueryValue` into a single-element JSON
/// array, so `IN`/`CONTAINS*` renderers can treat scalars uniformly.
trait IntoJsonValue {
    fn to_json_value(&self) -> serde_json::Value;
}

impl IntoJsonValue for QueryValue {
    fn to_json_value(&self) -> serde_json::Value {
        match self {
            QueryValue::String(s) => serde_json::Value::String(s.clone()),
            QueryValue::Number(n) => serde_json::Number::from_f64(*n)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            QueryValue::Bool(b) => serde_json::Value::Bool(*b),
            QueryValue::Null => serde_json::Value::Null,
            QueryValue::Array(a) => serde_json::Value::Array(a.clone()),
            QueryValue::DateTime(dt) => serde_json::Value::String(dt.to_rfc3339()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_by_id_includes_soft_delete_filter() {
        let sql = SqliteQueryAdapter::build_select_by_id("users", true);
        assert!(sql.contains("deleted_at IS NULL"));
        let sql2 = SqliteQueryAdapter::build_select_by_id("users", false);
        assert!(!sql2.contains("deleted_at"));
    }

    #[test]
    fn insert_uses_on_conflict_do_nothing() {
        let sql = SqliteQueryAdapter::build_insert("users");
        assert!(sql.contains("INSERT INTO users"));
        assert!(sql.contains("ON CONFLICT(id) DO NOTHING"));
        assert!(sql.contains("RETURNING"));
    }

    #[test]
    fn update_replaces_data_without_touching_version() {
        // version lives in the entity JSON (data), not a column — plain update
        // must not auto-increment anything (matches SurrealDB CONTENT update).
        let sql = SqliteQueryAdapter::build_update("users");
        assert!(sql.contains("SET data = ?, updated_at = ? WHERE id = ? AND deleted_at IS NULL"));
        assert!(!sql.contains("version"));
        assert!(sql.contains("RETURNING"));
    }

    #[test]
    fn update_with_version_checks_json_extract() {
        let sql = SqliteQueryAdapter::build_update_with_version("users");
        assert!(
            sql.contains("COALESCE(json_extract(data, '$.record_version'), json_extract(data, '$.entity_version'), json_extract(data, '$.version'), 0) = ?"),
            "optimistic lock must accept canonical, legacy, and pre-version JSON records, got: {sql}"
        );
        assert!(!sql.contains("version = version + 1"));
    }

    #[test]
    fn timestamp_guard_parses_instants_and_rejects_unsafe_fields() {
        let sql = SqliteQueryAdapter::build_update_with_version_and_timestamp_guard(
            "users",
            "policy.expires_at",
        )
        .unwrap();
        assert!(sql.contains("json_type(data, '$.policy.expires_at') IS NULL"));
        assert!(
            sql.contains("julianday(json_extract(data, '$.policy.expires_at')) > julianday('now')")
        );
        assert!(sql.contains("GLOB '[0-9][0-9][0-9][0-9]-"));
        assert!(
            SqliteQueryAdapter::build_update_with_version_and_timestamp_guard(
                "users",
                "expires_at'); DELETE FROM users; --"
            )
            .is_err()
        );
        assert!(
            SqliteQueryAdapter::build_update_with_version_and_timestamp_guard("users", "").is_err()
        );
    }

    #[test]
    fn expired_timestamp_guard_requires_valid_expired_value() {
        let sql = SqliteQueryAdapter::build_update_with_version_and_expired_timestamp_guard(
            "users",
            "expires_at",
        )
        .unwrap();
        assert!(sql.contains("json_type(data, '$.expires_at') = 'text'"));
        assert!(sql.contains("julianday(json_extract(data, '$.expires_at')) <= julianday('now')"));
        assert!(!sql.contains("json_type(data, '$.expires_at') IS NULL"));
    }

    #[test]
    fn delete_with_version_checks_json_extract() {
        let hard = SqliteQueryAdapter::build_delete_with_version("users");
        assert!(hard.starts_with("DELETE FROM users WHERE id = ?"));
        assert!(hard.contains(
            "COALESCE(json_extract(data, '$.record_version'), json_extract(data, '$.entity_version'), json_extract(data, '$.version'), 0) = ?"
        ));

        let soft = SqliteQueryAdapter::build_soft_delete_with_version("users");
        assert!(soft.starts_with("UPDATE users SET deleted_at = ?"));
        assert!(soft.contains("deleted_at IS NULL"));
        assert!(soft.contains(
            "COALESCE(json_extract(data, '$.record_version'), json_extract(data, '$.entity_version'), json_extract(data, '$.version'), 0) = ?"
        ));
    }

    #[test]
    fn query_builder_translates_fields_to_json_extract() {
        let builder = QueryBuilder::new()
            .eq("name", "John")
            .gt("age", 18)
            .limit(10)
            .offset(20);

        let q = SqliteQueryAdapter::build_select("users", &builder, false);

        assert!(
            q.sql.contains("json_extract(data, '$.name') = ?"),
            "name should use json_extract, got: {}",
            q.sql
        );
        assert!(
            q.sql.contains("json_extract(data, '$.age') > ?"),
            "age should use json_extract, got: {}",
            q.sql
        );
        assert!(q.sql.contains("LIMIT 10"));
        assert!(q.sql.contains("OFFSET 20"));
        assert_eq!(q.params.len(), 2);
    }

    #[test]
    fn soft_delete_parenthesizes_user_conditions() {
        // OR inside user conditions must be parenthesized so it doesn't leak
        // past the deleted_at guard.
        let builder = QueryBuilder::new()
            .eq("role", "admin")
            .or()
            .eq("role", "superadmin");

        let q = SqliteQueryAdapter::build_select("users", &builder, true);
        assert!(
            q.sql.contains("deleted_at IS NULL AND ("),
            "soft-delete + conds must parenthesize conds, got: {}",
            q.sql
        );
    }

    #[test]
    fn order_by_uses_json_extract_for_entity_fields() {
        let builder = QueryBuilder::new().order_by("name", SortDirection::Asc);
        let q = SqliteQueryAdapter::build_select("items", &builder, false);
        assert!(
            q.sql.contains("ORDER BY json_extract(data, '$.name') ASC"),
            "got: {}",
            q.sql
        );

        // metadata column should NOT be wrapped in json_extract
        let builder2 = QueryBuilder::new().order_by("created_at", SortDirection::Desc);
        let q2 = SqliteQueryAdapter::build_select("items", &builder2, false);
        assert!(
            q2.sql.contains("ORDER BY created_at DESC"),
            "got: {}",
            q2.sql
        );
    }

    #[test]
    fn in_operator_expands_placeholders() {
        let builder = QueryBuilder::new().in_array("status", vec!["active", "pending"]);
        let q = SqliteQueryAdapter::build_select("items", &builder, false);
        assert!(q.sql.contains("IN (?, ?)"), "got: {}", q.sql);
        assert_eq!(q.params.len(), 2);
    }

    #[test]
    fn contains_uses_json_each() {
        let builder = QueryBuilder::new().contains("tags", "hvac");
        let q = SqliteQueryAdapter::build_select("items", &builder, false);
        assert!(
            q.sql.contains("json_each(json_extract(data, '$.tags'))"),
            "got: {}",
            q.sql
        );
        assert_eq!(q.params.len(), 1);
    }

    #[test]
    fn count_filtered_uses_count_star() {
        let builder = QueryBuilder::new().eq("status", "active");
        let q = SqliteQueryAdapter::build_count_filtered("items", &builder, false);
        assert!(q.sql.contains("SELECT COUNT(*)"), "got: {}", q.sql);
        assert!(q.sql.contains("json_extract(data, '$.status') = ?"));
    }

    #[test]
    fn encoded_metadata_query_unwraps_json_without_assigning_ownership() {
        let builder = QueryBuilder::new()
            .eq("encoded_metadata.organization_id", "org-a")
            .order_by("id", SortDirection::Asc)
            .limit(20)
            .offset(40);
        let q = SqliteQueryAdapter::build_select("ml_models", &builder, false);
        assert!(q
            .sql
            .contains("json_valid(json_extract(data, '$.metadata'))"));
        assert!(q.sql.contains("'$.organization_id'"));
        assert!(!q.sql.contains("COALESCE"));
        assert!(!q.sql.contains("'default'"));
        assert!(q.sql.contains("ORDER BY id ASC"));
        assert!(q.sql.contains("LIMIT 20"));
        assert!(q.sql.contains("OFFSET 40"));
        assert_eq!(q.params.len(), 1);
    }

    #[tokio::test]
    async fn missing_or_aliased_encoded_owner_remains_absent_in_sqlite() {
        let pool = crate::open_in_memory().await.unwrap();
        sqlx::query("CREATE TABLE entries (id TEXT PRIMARY KEY, data TEXT NOT NULL)")
            .execute(pool.as_ref())
            .await
            .unwrap();
        for (id, data) in [
            ("missing", serde_json::json!({"title": "old"})),
            (
                "snake",
                serde_json::json!({"metadata": r#"{"organization_id":"org-a"}"#}),
            ),
            (
                "camel",
                serde_json::json!({"metadata": r#"{"organizationId":"org-b"}"#}),
            ),
            ("malformed", serde_json::json!({"metadata": "not-json"})),
        ] {
            sqlx::query("INSERT INTO entries (id, data) VALUES (?, ?)")
                .bind(id)
                .bind(data.to_string())
                .execute(pool.as_ref())
                .await
                .unwrap();
        }
        let expr = SqliteQueryAdapter::field_expr("encoded_metadata.organization_id");
        let sql = format!("SELECT {expr} FROM entries WHERE id = ?");
        for (id, expected) in [
            ("missing", None),
            ("snake", Some("org-a")),
            ("camel", None),
            ("malformed", None),
        ] {
            let owner: Option<String> = sqlx::query_scalar(&sql)
                .bind(id)
                .fetch_one(pool.as_ref())
                .await
                .unwrap();
            assert_eq!(owner.as_deref(), expected, "row {id}");
        }
    }

    #[test]
    fn sanitize_strips_injection() {
        assert_eq!(SqliteQueryAdapter::sanitize_identifier("users"), "users");
        assert_eq!(
            SqliteQueryAdapter::sanitize_identifier("users; DROP TABLE--"),
            "usersDROPTABLE"
        );
    }
}
