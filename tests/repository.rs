//! Integration tests for `SqliteRepository` against an in-memory SQLite DB.
//!
//! Each test gets its own isolated in-memory database via `open_in_memory()`.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use univers_aip_contracts_data::storage::{QueryBuilder, RepositoryTrait, SortDirection};
use univers_aip_lib_storage_sqlite::{open_in_memory, SqliteRepository, SqliteWriteCoordinator};

/// Full entity (id injected on read; version comes from `data`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestEntity {
    id: String,
    version: i64,
    name: String,
    email: String,
    age: i64,
    tags: Vec<String>,
}

/// Data shape (no id; version lives here — matches SurrealDB semantics).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestEntityData {
    version: i64,
    name: String,
    email: String,
    age: i64,
    tags: Vec<String>,
}

impl TestEntityData {
    fn new(name: &str, email: &str, age: i64) -> Self {
        Self {
            version: 0,
            name: name.to_string(),
            email: email.to_string(),
            age,
            tags: Vec::new(),
        }
    }
}

type Repo = SqliteRepository<TestEntity, TestEntityData>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyEntity {
    id: String,
    #[serde(default)]
    version: i64,
    name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyEntityData {
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<i64>,
    name: String,
}

type LegacyRepo = SqliteRepository<LegacyEntity, LegacyEntityData>;

type JsonRepo = SqliteRepository<serde_json::Value, serde_json::Value>;

async fn setup() -> Repo {
    let pool = open_in_memory().await.expect("open in-memory pool");
    SqliteRepository::new(pool, "users")
}

async fn setup_soft() -> Repo {
    let pool = open_in_memory().await.expect("open in-memory pool");
    SqliteRepository::with_soft_delete(pool, "users")
}

async fn setup_legacy() -> LegacyRepo {
    let pool = open_in_memory().await.expect("open in-memory pool");
    SqliteRepository::new(pool, "legacy_records")
}

async fn setup_json() -> JsonRepo {
    let pool = open_in_memory().await.expect("open in-memory pool");
    SqliteRepository::new(pool, "guarded_records")
}

/// Strip the `"table:"` prefix to get the bare id.
fn bare(id: &str) -> &str {
    id.rsplit(':').next().unwrap_or(id)
}

// ---------------------------------------------------------------------------
// Create / read round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_returns_table_prefixed_id_and_version_zero() {
    let repo = setup().await;
    let e = repo
        .create(TestEntityData::new("Ada", "ada@x.com", 36))
        .await
        .unwrap();

    assert!(
        e.id.starts_with("users:"),
        "id should be table-prefixed: {}",
        e.id
    );
    assert_eq!(e.version, 0);
    assert_eq!(e.name, "Ada");
}

#[tokio::test]
async fn find_by_id_resolves_both_bare_and_prefixed_forms() {
    let repo = setup().await;
    let e = repo
        .create(TestEntityData::new("Ada", "ada@x.com", 36))
        .await
        .unwrap();

    // bare id (after prefix)
    let by_bare = repo.find_by_id(bare(&e.id)).await.unwrap();
    assert_eq!(by_bare.as_ref().unwrap(), &e);

    // full "table:id" form
    let by_prefixed = repo.find_by_id(&e.id).await.unwrap();
    assert_eq!(by_prefixed.as_ref().unwrap(), &e);
}

#[tokio::test]
async fn find_by_missing_id_returns_none() {
    let repo = setup().await;
    let miss = repo.find_by_id("nope").await.unwrap();
    assert!(miss.is_none());
}

// ---------------------------------------------------------------------------
// create_with_id + idempotency
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_with_id_is_idempotent_on_duplicate() {
    let repo = setup().await;
    let first = repo
        .create_with_id("abc", TestEntityData::new("Ada", "ada@x.com", 36))
        .await
        .unwrap();
    assert!(first.is_some());

    // Same id again — must NOT error; returns None (idempotent).
    let dup = repo
        .create_with_id("abc", TestEntityData::new("Other", "o@x.com", 1))
        .await
        .unwrap();
    assert!(
        dup.is_none(),
        "duplicate create_with_id must return None, got {dup:?}"
    );

    // Original row is intact.
    let got = repo.find_by_id("abc").await.unwrap().unwrap();
    assert_eq!(got.name, "Ada");
}

// ---------------------------------------------------------------------------
// Update + optimistic locking
// ---------------------------------------------------------------------------

#[tokio::test]
async fn update_replaces_data_version_follows_data() {
    let repo = setup().await;
    let mut d0 = TestEntityData::new("Ada", "ada@x.com", 36);
    d0.version = 2;
    let e = repo.create_with_id("u1", d0).await.unwrap().unwrap();
    assert_eq!(e.version, 2, "version comes from the entity data");

    // Plain update replaces `data` wholesale — version is whatever the new
    // data carries (no storage-side auto-increment, matching SurrealDB).
    let mut d1 = TestEntityData::new("Ada2", "ada2@x.com", 37);
    d1.version = 3;
    let updated = repo.update("u1", d1).await.unwrap().unwrap();
    assert_eq!(updated.name, "Ada2");
    assert_eq!(updated.age, 37);
    assert_eq!(updated.version, 3, "version follows the new data");
}

#[tokio::test]
async fn update_missing_returns_none() {
    let repo = setup().await;
    let r = repo
        .update("ghost", TestEntityData::new("x", "x@x.com", 1))
        .await
        .unwrap();
    assert!(r.is_none());
}

#[tokio::test]
async fn update_with_version_succeeds_on_match_and_fails_on_stale() {
    let repo = setup().await;
    let mut d = TestEntityData::new("Ada", "ada@x.com", 36);
    d.version = 5;
    repo.create_with_id("u2", d).await.unwrap(); // stored $.version == 5

    // Correct expected (5) -> updates; new data carries version 6.
    let mut next = TestEntityData::new("B", "b@x.com", 1);
    next.version = 6;
    let ok = repo
        .update_with_version("u2", next, 5)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ok.version, 6);

    // Stale expected (5 again, but stored is now 6) -> None.
    let mut stale = TestEntityData::new("C", "c@x.com", 2);
    stale.version = 7;
    let stale_res = repo.update_with_version("u2", stale, 5).await.unwrap();
    assert!(stale_res.is_none(), "stale version must yield None");

    // Wrong id -> None.
    let missing = repo
        .update_with_version("ghost", TestEntityData::new("D", "d@x.com", 3), 0)
        .await
        .unwrap();
    assert!(missing.is_none());
}

#[tokio::test]
async fn update_with_version_treats_pre_version_json_as_version_zero() {
    let repo = setup_legacy().await;
    repo.create_with_id(
        "legacy-1",
        LegacyEntityData {
            version: None,
            name: "Before".to_string(),
        },
    )
    .await
    .expect("legacy create should work");

    let updated = repo
        .update_with_version(
            "legacy-1",
            LegacyEntityData {
                version: Some(1),
                name: "After".to_string(),
            },
            0,
        )
        .await
        .expect("legacy CAS should work")
        .expect("version zero should match missing version");

    assert_eq!(updated.version, 1);
    assert_eq!(updated.name, "After");
    assert!(repo
        .update_with_version(
            "legacy-1",
            LegacyEntityData {
                version: Some(2),
                name: "Stale".to_string(),
            },
            0,
        )
        .await
        .expect("stale legacy CAS should be observable")
        .is_none());
}

// Risk: a deadline checked before a CAS write can expire before that write
// linearizes. The repository predicate must fence both facts in one statement.
#[tokio::test]
async fn timestamp_guard_is_atomic_strict_and_fail_closed() {
    use chrono::{Duration, Utc};
    use serde_json::json;

    let now = Utc::now();
    let cases = [
        ("missing", json!({"version": 1}), true),
        ("null", json!({"version": 1, "expires_at": null}), true),
        (
            "future-offset",
            json!({"version": 1, "expires_at": now + Duration::hours(1)}),
            true,
        ),
        (
            "past",
            json!({"version": 1, "expires_at": now - Duration::hours(1)}),
            false,
        ),
        (
            "malformed",
            json!({"version": 1, "expires_at": "tomorrow"}),
            false,
        ),
        ("non-string", json!({"version": 1, "expires_at": 42}), false),
    ];

    let repo = setup_json().await;
    for (id, stored, should_update) in cases {
        repo.create_with_id(id, stored).await.unwrap().unwrap();
        let updated = repo
            .update_with_version_and_timestamp_guard(
                id,
                json!({"version": 2, "result": "accepted"}),
                1,
                "expires_at",
            )
            .await
            .unwrap();
        assert_eq!(updated.is_some(), should_update, "case {id}");
    }

    let stale = repo
        .update_with_version_and_timestamp_guard("missing", json!({"version": 3}), 1, "expires_at")
        .await
        .unwrap();
    assert!(
        stale.is_none(),
        "version fence must remain part of the statement"
    );

    let error = repo
        .update_with_version_and_timestamp_guard(
            "null",
            json!({"version": 3}),
            2,
            "expires_at'); DELETE FROM guarded_records; --",
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        univers_aip_contracts_data::storage::RepositoryError::Validation(_)
    ));

    for id in [
        "missing",
        "null",
        "future-offset",
        "malformed",
        "non-string",
    ] {
        let expired = repo
            .update_with_version_and_expired_timestamp_guard(
                id,
                json!({"version": 2, "result": "expired"}),
                1,
                "expires_at",
            )
            .await
            .unwrap();
        assert!(expired.is_none(), "expiry case {id} must fail closed");
    }
    let expired = repo
        .update_with_version_and_expired_timestamp_guard(
            "past",
            json!({"version": 2, "result": "expired"}),
            1,
            "expires_at",
        )
        .await
        .unwrap();
    assert!(expired.is_some());
}

// Risk: a stale actor must not delete a row changed after it was read.
#[tokio::test]
async fn delete_with_version_is_atomic_for_hard_and_soft_delete() {
    for repo in [setup().await, setup_soft().await] {
        let mut original = TestEntityData::new("Ada", "ada@x.com", 36);
        original.version = 5;
        repo.create_with_id("u3", original).await.unwrap();

        let stale = repo.delete_with_version("u3", 4).await.unwrap();
        assert!(stale.is_none());
        assert!(repo.find_by_id("u3").await.unwrap().is_some());

        let deleted = repo.delete_with_version("u3", 5).await.unwrap();
        assert_eq!(deleted.as_ref().map(|entity| entity.version), Some(5));
        assert!(repo.find_by_id("u3").await.unwrap().is_none());

        let repeated = repo.delete_with_version("u3", 5).await.unwrap();
        assert!(repeated.is_none());
    }
}

// ---------------------------------------------------------------------------
// List / count / exists
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_count_exists() {
    let repo = setup().await;
    repo.create_with_id("a", TestEntityData::new("A", "a@x.com", 10))
        .await
        .unwrap();
    repo.create_with_id("b", TestEntityData::new("B", "b@x.com", 20))
        .await
        .unwrap();
    repo.create_with_id("c", TestEntityData::new("C", "c@x.com", 30))
        .await
        .unwrap();

    assert_eq!(repo.count().await.unwrap(), 3);
    assert_eq!(repo.list().await.unwrap().len(), 3);
    assert!(repo.exists("a").await.unwrap());
    assert!(!repo.exists("zzz").await.unwrap());
}

// Risk: concurrent first use must share schema initialization instead of
// competing on SQLite DDL or deadlocking callers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_first_use_initializes_the_table_once() {
    let repo = Arc::new(setup().await);
    let mut tasks = tokio::task::JoinSet::new();

    for _ in 0..32 {
        let repo = Arc::clone(&repo);
        tasks.spawn(async move { repo.count().await });
    }

    while let Some(result) = tasks.join_next().await {
        assert_eq!(result.expect("count task must not panic").unwrap(), 0);
    }
}

// Risk: sibling repositories sharing one SQLite store must queue writes before
// reaching SQLite's busy timeout, including under concurrent first use.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_write_coordinator_serializes_sibling_repositories() {
    let pool = open_in_memory().await.unwrap();
    let coordinator = SqliteWriteCoordinator::default();
    let first = Arc::new(
        SqliteRepository::<TestEntity, TestEntityData>::with_write_coordinator(
            pool.clone(),
            "users",
            coordinator.clone(),
        ),
    );
    let second = Arc::new(
        SqliteRepository::<TestEntity, TestEntityData>::with_write_coordinator(
            pool,
            "other_users",
            coordinator.clone(),
        ),
    );
    let permit = coordinator.acquire().await;

    let first_write = tokio::spawn(async move {
        first
            .create(TestEntityData::new("Ada", "ada@example.com", 36))
            .await
    });
    let second_write = tokio::spawn(async move {
        second
            .create(TestEntityData::new("Grace", "grace@example.com", 37))
            .await
    });
    tokio::task::yield_now().await;
    assert!(!first_write.is_finished());
    assert!(!second_write.is_finished());

    drop(permit);
    first_write.await.unwrap().unwrap();
    second_write.await.unwrap().unwrap();
}

#[tokio::test]
async fn find_by_ids_batch_lookup() {
    let repo = setup().await;
    repo.create_with_id("a", TestEntityData::new("A", "a@x.com", 10))
        .await
        .unwrap();
    repo.create_with_id("b", TestEntityData::new("B", "b@x.com", 20))
        .await
        .unwrap();
    repo.create_with_id("c", TestEntityData::new("C", "c@x.com", 30))
        .await
        .unwrap();

    let got = repo.find_by_ids(vec!["a", "c", "missing"]).await.unwrap();
    assert_eq!(got.len(), 2, "missing id is skipped");
    let names: Vec<_> = got.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"A") && names.contains(&"C"));
}

#[tokio::test]
async fn find_by_ids_empty() {
    let repo = setup().await;
    let got = repo.find_by_ids(vec![]).await.unwrap();
    assert!(got.is_empty());
}

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_paginated_slices_and_reports_totals() {
    let repo = setup().await;
    for i in 0..5 {
        repo.create_with_id(&format!("u{i}"), TestEntityData::new("N", "e@x.com", i))
            .await
            .unwrap();
    }

    let p = repo.list_paginated(0, 2).await.unwrap();
    assert_eq!(p.items.len(), 2);
    assert_eq!(p.total, 5);
    assert_eq!(p.total_pages, 3);
    assert_eq!(p.page, 0);

    let last = repo.list_paginated(2, 2).await.unwrap();
    assert_eq!(last.items.len(), 1, "last page has the remainder");
    assert_eq!(last.total_pages, 3);
}

#[tokio::test]
async fn list_paginated_empty_db() {
    let repo = setup().await;
    let p = repo.list_paginated(0, 10).await.unwrap();
    assert!(p.items.is_empty());
    assert_eq!(p.total, 0);
    assert_eq!(p.total_pages, 0);
}

// ---------------------------------------------------------------------------
// query_safe
// ---------------------------------------------------------------------------

#[tokio::test]
async fn query_safe_eq_string() {
    let repo = setup().await;
    repo.create_with_id("a", TestEntityData::new("Ada", "a@x.com", 10))
        .await
        .unwrap();
    repo.create_with_id("b", TestEntityData::new("Bob", "b@x.com", 20))
        .await
        .unwrap();

    let r = repo
        .query_safe(QueryBuilder::new().eq("name", "Ada"))
        .await
        .unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].name, "Ada");
}

#[tokio::test]
async fn query_safe_gt_number() {
    let repo = setup().await;
    repo.create_with_id("a", TestEntityData::new("A", "a@x.com", 10))
        .await
        .unwrap();
    repo.create_with_id("b", TestEntityData::new("B", "b@x.com", 20))
        .await
        .unwrap();
    repo.create_with_id("c", TestEntityData::new("C", "c@x.com", 30))
        .await
        .unwrap();

    let r = repo
        .query_safe(QueryBuilder::new().gt("age", 15))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "age > 15 -> B and C");
}

#[tokio::test]
async fn query_safe_like() {
    let repo = setup().await;
    repo.create_with_id("a", TestEntityData::new("A", "ada@example.com", 10))
        .await
        .unwrap();
    repo.create_with_id("b", TestEntityData::new("B", "bob@other.io", 20))
        .await
        .unwrap();

    let r = repo
        .query_safe(QueryBuilder::new().like("email", "%example.com"))
        .await
        .unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].email, "ada@example.com");
}

#[tokio::test]
async fn query_safe_in_array() {
    let repo = setup().await;
    repo.create_with_id("a", TestEntityData::new("A", "a@x.com", 10))
        .await
        .unwrap();
    repo.create_with_id("b", TestEntityData::new("B", "b@x.com", 20))
        .await
        .unwrap();
    repo.create_with_id("c", TestEntityData::new("C", "c@x.com", 30))
        .await
        .unwrap();

    let r = repo
        .query_safe(QueryBuilder::new().in_array("age", vec![10, 30]))
        .await
        .unwrap();
    assert_eq!(r.len(), 2);
}

#[tokio::test]
async fn query_safe_or_conditions() {
    let repo = setup().await;
    repo.create_with_id("a", TestEntityData::new("A", "a@x.com", 10))
        .await
        .unwrap();
    repo.create_with_id("b", TestEntityData::new("B", "b@x.com", 20))
        .await
        .unwrap();
    repo.create_with_id("c", TestEntityData::new("C", "c@x.com", 30))
        .await
        .unwrap();

    // name = A OR age > 25 -> A and C
    let r = repo
        .query_safe(QueryBuilder::new().eq("name", "A").or().gt("age", 25))
        .await
        .unwrap();
    assert_eq!(r.len(), 2);
}

#[tokio::test]
async fn query_safe_order_by_entity_field() {
    let repo = setup().await;
    repo.create_with_id("a", TestEntityData::new("C", "c@x.com", 1))
        .await
        .unwrap();
    repo.create_with_id("b", TestEntityData::new("A", "a@x.com", 2))
        .await
        .unwrap();
    repo.create_with_id("c", TestEntityData::new("B", "b@x.com", 3))
        .await
        .unwrap();

    let r = repo
        .query_safe(QueryBuilder::new().order_by("name", SortDirection::Asc))
        .await
        .unwrap();
    let names: Vec<_> = r.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(names, vec!["A", "B", "C"]);
}

#[tokio::test]
async fn count_filtered_uses_count_query() {
    let repo = setup().await;
    for i in 0..4 {
        repo.create_with_id(&format!("u{i}"), TestEntityData::new("N", "e@x.com", i))
            .await
            .unwrap();
    }
    let query = QueryBuilder::new()
        .gte("age", 2)
        .order_by("age", SortDirection::Desc)
        .offset(1)
        .limit(1);
    assert_eq!(repo.query_safe(query.clone()).await.unwrap().len(), 1);
    let n = repo.count_filtered(query).await.unwrap();
    assert_eq!(n, 2);
}

#[tokio::test]
async fn encoded_metadata_scope_query_filters_and_pages_legacy_json_metadata() {
    let repo = setup_json().await;
    repo.create_with_id(
        "model-a",
        serde_json::json!({
            "name": "A",
            "status": "trained",
            "metadata": r#"{"organization_id":"org-a"}"#,
        }),
    )
    .await
    .unwrap();
    repo.create_with_id(
        "model-b",
        serde_json::json!({
            "name": "B",
            "status": "trained",
            "metadata": r#"{"organizationId":"org-b"}"#,
        }),
    )
    .await
    .unwrap();
    repo.create_with_id(
        "model-default",
        serde_json::json!({
            "name": "Default",
            "status": "trained",
            "metadata": "{}",
        }),
    )
    .await
    .unwrap();

    let scope = QueryBuilder::new().eq("encoded_metadata.organization_id", "org-a");
    assert_eq!(repo.count_filtered(scope.clone()).await.unwrap(), 1);
    let page = repo
        .query_safe(scope.order_by("id", SortDirection::Asc).limit(1).offset(0))
        .await
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0]["name"], "A");
}

// ---------------------------------------------------------------------------
// Batch ops
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_batch_inserts_all() {
    let repo = setup().await;
    let items = vec![
        TestEntityData::new("A", "a@x.com", 1),
        TestEntityData::new("B", "b@x.com", 2),
        TestEntityData::new("C", "c@x.com", 3),
    ];
    let created = repo.create_batch(items).await.unwrap();
    assert_eq!(created.len(), 3);
    assert_eq!(repo.count().await.unwrap(), 3);
    for e in &created {
        assert!(e.id.starts_with("users:"));
        assert_eq!(e.version, 0);
    }
}

#[tokio::test]
async fn create_batch_empty() {
    let repo = setup().await;
    let created = repo.create_batch(vec![]).await.unwrap();
    assert!(created.is_empty());
}

#[tokio::test]
async fn delete_batch_returns_removed_entities() {
    let repo = setup().await;
    repo.create_with_id("a", TestEntityData::new("A", "a@x.com", 1))
        .await
        .unwrap();
    repo.create_with_id("b", TestEntityData::new("B", "b@x.com", 2))
        .await
        .unwrap();

    let removed = repo.delete_batch(vec!["a", "ghost"]).await.unwrap();
    assert_eq!(removed.len(), 2);
    assert!(removed[0].is_some(), "existing row returned");
    assert!(removed[1].is_none(), "missing row -> None");
    assert_eq!(repo.count().await.unwrap(), 1);
}

// Risk: lifecycle cleanup must not durably remove only the first records when
// a later delete fails. One failed batch must leave every target retryable.
#[tokio::test]
async fn delete_batch_rolls_back_every_row_when_one_delete_fails() {
    let repo = setup().await;
    repo.create_with_id("a", TestEntityData::new("A", "a@x.com", 1))
        .await
        .unwrap();
    repo.create_with_id("b", TestEntityData::new("B", "b@x.com", 2))
        .await
        .unwrap();
    sqlx::query(
        "CREATE TRIGGER reject_b_delete BEFORE DELETE ON users \
         WHEN OLD.id = 'b' BEGIN SELECT RAISE(ABORT, 'reject b'); END",
    )
    .execute(repo.pool())
    .await
    .unwrap();

    let error = repo
        .delete_batch(vec!["a", "b"])
        .await
        .expect_err("the trigger must reject the batch");
    assert!(error.to_string().contains("reject b"));
    assert!(repo.find_by_id("a").await.unwrap().is_some());
    assert!(repo.find_by_id("b").await.unwrap().is_some());
}

// ---------------------------------------------------------------------------
// Hard vs soft delete
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hard_delete_removes_row() {
    let repo = setup().await; // hard delete
    repo.create_with_id("a", TestEntityData::new("A", "a@x.com", 1))
        .await
        .unwrap();
    let removed = repo.delete("a").await.unwrap();
    assert!(removed.is_some());
    assert!(repo.find_by_id("a").await.unwrap().is_none());
    assert_eq!(repo.count().await.unwrap(), 0);
}

#[tokio::test]
async fn soft_delete_hides_row_from_reads() {
    let repo = setup_soft().await;
    repo.create_with_id("a", TestEntityData::new("A", "a@x.com", 1))
        .await
        .unwrap();
    assert_eq!(repo.count().await.unwrap(), 1);

    let removed = repo.delete("a").await.unwrap();
    assert!(removed.is_some(), "soft delete returns the entity");

    // Reads must exclude the soft-deleted row.
    assert!(repo.find_by_id("a").await.unwrap().is_none());
    assert_eq!(repo.list().await.unwrap().len(), 0);
    assert_eq!(repo.count().await.unwrap(), 0);
}

#[tokio::test]
async fn soft_delete_is_terminal_for_retries_updates_and_recreation() {
    let repo = setup_soft().await;
    repo.create_with_id("terminal", TestEntityData::new("A", "a@x.com", 1))
        .await
        .unwrap();

    assert!(repo.delete("terminal").await.unwrap().is_some());
    assert!(repo.delete("terminal").await.unwrap().is_none());
    assert!(repo
        .update(
            "terminal",
            TestEntityData::new("rewritten", "rewrite@x.com", 2),
        )
        .await
        .unwrap()
        .is_none());
    assert!(repo
        .update_with_version(
            "terminal",
            TestEntityData::new("rewritten", "rewrite@x.com", 2),
            0,
        )
        .await
        .unwrap()
        .is_none());
    assert!(repo
        .create_with_id("terminal", TestEntityData::new("resurrected", "x@x.com", 3))
        .await
        .unwrap()
        .is_none());
}

// ---------------------------------------------------------------------------
// Array field: CONTAINS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn query_safe_contains_array_field() {
    let repo = setup().await;
    let mut d1 = TestEntityData::new("A", "a@x.com", 1);
    d1.tags = vec!["hvac".into(), "ctrl".into()];
    let mut d2 = TestEntityData::new("B", "b@x.com", 2);
    d2.tags = vec!["lighting".into()];
    repo.create_with_id("a", d1).await.unwrap();
    repo.create_with_id("b", d2).await.unwrap();

    let r = repo
        .query_safe(QueryBuilder::new().contains("tags", "hvac"))
        .await
        .unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].name, "A");
}

// ---------------------------------------------------------------------------
// Trait-object safety (dyn RepositoryTrait)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn works_as_trait_object() {
    let pool = open_in_memory().await.expect("pool");
    let repo: std::sync::Arc<dyn RepositoryTrait<TestEntity, TestEntityData>> =
        std::sync::Arc::new(SqliteRepository::new(pool, "users"));

    let e = repo
        .create(TestEntityData::new("Ada", "ada@x.com", 36))
        .await
        .unwrap();
    assert!(repo.exists(bare(&e.id)).await.unwrap());
}

// ---------------------------------------------------------------------------
// Dynamic where-filter reads (judge / goal-eval resolver path)
// ---------------------------------------------------------------------------

/// Scalar equality through `build_json_where` must match native JSON values:
/// `json_extract` unwraps scalars, so the bound side must unwrap too
/// (regression: `json(?)` kept the quoted form and matched nothing).
#[tokio::test]
async fn where_filter_matches_scalar_values() {
    let pool = open_in_memory().await.expect("pool");
    let repo: Repo = SqliteRepository::new(pool.clone(), "users");
    repo.create(TestEntityData::new("Ada", "ada@x.com", 36))
        .await
        .unwrap();
    repo.create(TestEntityData::new("Bob", "bob@x.com", 41))
        .await
        .unwrap();

    let by_name = std::collections::HashMap::from([("name".to_string(), serde_json::json!("Ada"))]);
    let n = univers_aip_lib_storage_sqlite::count_by_conditions(&pool, "users", &by_name)
        .await
        .unwrap();
    assert_eq!(n, 1, "string where-filter must match");

    let by_age = std::collections::HashMap::from([("age".to_string(), serde_json::json!(41))]);
    let n = univers_aip_lib_storage_sqlite::count_by_conditions(&pool, "users", &by_age)
        .await
        .unwrap();
    assert_eq!(n, 1, "numeric where-filter must match");

    let email = univers_aip_lib_storage_sqlite::query_latest_by_conditions(
        &pool, "users", &by_name, "email",
    )
    .await
    .unwrap();
    assert_eq!(email, Some(serde_json::json!("ada@x.com")));

    sqlx::query(
        "INSERT INTO users (id, data, created_at, updated_at) \
         VALUES (?, ?, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'), \
         (?, ?, '2026-01-02T00:00:00Z', '2026-01-02T00:00:00Z')",
    )
    .bind("old")
    .bind(
        serde_json::json!({
            "name": "Nested",
            "createdAt": "2026-01-01T00:00:00Z",
            "finalState": { "COP": 1.0 }
        })
        .to_string(),
    )
    .bind("new")
    .bind(
        serde_json::json!({
            "name": "Nested",
            "createdAt": "2026-01-02T00:00:00Z",
            "finalState": { "COP": 2.5 }
        })
        .to_string(),
    )
    .execute(&*pool)
    .await
    .unwrap();
    let by_nested =
        std::collections::HashMap::from([("name".to_string(), serde_json::json!("Nested"))]);
    let cop = univers_aip_lib_storage_sqlite::query_latest_by_conditions(
        &pool,
        "users",
        &by_nested,
        "finalState.COP",
    )
    .await
    .unwrap();
    assert_eq!(cop, Some(serde_json::json!(2.5)));
}

// ---------------------------------------------------------------------------
// Dynamic delete primitives (session-subtree purge path)
// ---------------------------------------------------------------------------

/// `delete_by_conditions` removes only matching rows and reports the count;
/// an empty filter must be refused (never a whole-table delete).
#[tokio::test]
async fn delete_by_conditions_deletes_matches_only() {
    let pool = open_in_memory().await.expect("pool");
    let repo: Repo = SqliteRepository::new(pool.clone(), "users");
    repo.create(TestEntityData::new("Ada", "ada@x.com", 36))
        .await
        .unwrap();
    repo.create(TestEntityData::new("Ada", "ada2@x.com", 37))
        .await
        .unwrap();
    repo.create(TestEntityData::new("Bob", "bob@x.com", 41))
        .await
        .unwrap();

    let by_name = std::collections::HashMap::from([("name".to_string(), serde_json::json!("Ada"))]);
    let n = univers_aip_lib_storage_sqlite::delete_by_conditions(&pool, "users", &by_name)
        .await
        .unwrap();
    assert_eq!(n, 2, "both Ada rows removed");

    let empty: std::collections::HashMap<String, serde_json::Value> =
        std::collections::HashMap::new();
    let remaining = univers_aip_lib_storage_sqlite::count_by_conditions(&pool, "users", &empty)
        .await
        .unwrap();
    assert_eq!(remaining, 1, "Bob survives");

    // Whole-table delete refused.
    assert!(
        univers_aip_lib_storage_sqlite::delete_by_conditions(&pool, "users", &empty)
            .await
            .is_err(),
        "empty filter must be rejected"
    );
}

/// `delete_by_tag` removes rows whose `data.tags` array contains the tag,
/// leaves everything else, and refuses an empty tag.
#[tokio::test]
async fn delete_by_tag_matches_array_membership() {
    let pool = open_in_memory().await.expect("pool");
    let repo: Repo = SqliteRepository::new(pool.clone(), "users");
    let mut tagged = TestEntityData::new("Gen1", "g1@x.com", 1);
    tagged.tags = vec!["session:abc".into(), "agent:007".into()];
    repo.create(tagged).await.unwrap();
    let mut other = TestEntityData::new("Gen2", "g2@x.com", 2);
    other.tags = vec!["session:xyz".into()];
    repo.create(other).await.unwrap();
    repo.create(TestEntityData::new("NoTags", "n@x.com", 3))
        .await
        .unwrap();

    let n = univers_aip_lib_storage_sqlite::delete_by_tag(&pool, "users", "session:abc")
        .await
        .unwrap();
    assert_eq!(n, 1, "only the session:abc row removed");

    let empty: std::collections::HashMap<String, serde_json::Value> =
        std::collections::HashMap::new();
    let remaining = univers_aip_lib_storage_sqlite::count_by_conditions(&pool, "users", &empty)
        .await
        .unwrap();
    assert_eq!(remaining, 2, "other-session + untagged rows survive");

    assert!(
        univers_aip_lib_storage_sqlite::delete_by_tag(&pool, "users", "")
            .await
            .is_err(),
        "empty tag must be rejected"
    );
}

#[tokio::test]
async fn namespaced_record_keys_round_trip_without_colliding_on_their_suffix() {
    let pool = open_in_memory().await.unwrap();
    let repo = JsonRepo::new(pool.clone(), "workflows");
    for (key, name) in [
        ("workflows:planning:proposal_lifecycle", "planning"),
        ("inspection:proposal_lifecycle", "inspection"),
        ("proposal_lifecycle", "legacy bare key"),
    ] {
        let created = repo
            .create_with_id(key, serde_json::json!({"name":name,"version":0}))
            .await
            .unwrap()
            .unwrap();
        let bare = key.strip_prefix("workflows:").unwrap_or(key);
        assert_eq!(created["id"], format!("workflows:{bare}"));
        assert_eq!(repo.find_by_id(bare).await.unwrap(), Some(created.clone()));
        assert_eq!(
            repo.find_by_id(&format!("workflows:{bare}")).await.unwrap(),
            Some(created)
        );
    }
    assert_eq!(repo.count().await.unwrap(), 3);
    assert!(repo
        .create_with_id(
            "planning:proposal_lifecycle",
            serde_json::json!({"version":0})
        )
        .await
        .unwrap()
        .is_none());
    let updated = repo
        .update_with_version(
            "workflows:planning:proposal_lifecycle",
            serde_json::json!({"name":"granted","version":1}),
            0,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated["id"], "workflows:planning:proposal_lifecycle");
    assert!(repo
        .update_with_version(
            "planning:proposal_lifecycle",
            serde_json::json!({"version":2}),
            0
        )
        .await
        .unwrap()
        .is_none());
    let reopened = JsonRepo::new(pool, "workflows");
    assert_eq!(
        reopened
            .find_by_id("planning:proposal_lifecycle")
            .await
            .unwrap()
            .unwrap()["name"],
        "granted"
    );
    assert_eq!(
        reopened
            .find_by_id("inspection:proposal_lifecycle")
            .await
            .unwrap()
            .unwrap()["name"],
        "inspection"
    );
    assert_eq!(
        reopened
            .find_by_ids(vec![
                "planning:proposal_lifecycle",
                "workflows:inspection:proposal_lifecycle"
            ])
            .await
            .unwrap()
            .len(),
        2
    );
    assert!(reopened
        .delete_with_version("workflows:planning:proposal_lifecycle", 1)
        .await
        .unwrap()
        .is_some());
    assert!(reopened
        .find_by_id("planning:proposal_lifecycle")
        .await
        .unwrap()
        .is_none());
    assert!(reopened
        .find_by_id("inspection:proposal_lifecycle")
        .await
        .unwrap()
        .is_some());
    assert!(reopened
        .find_by_id("proposal_lifecycle")
        .await
        .unwrap()
        .is_some());
}
