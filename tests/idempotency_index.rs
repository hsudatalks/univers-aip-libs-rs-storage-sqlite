//! The task idempotency-key uniqueness backstop.
//!
//! Proves the partial unique expression index (defined in
//! `univers-task-trait`) actually dedups at the storage layer: at most one
//! LIVE row per `(organization_id, created_by, idempotency_key)`, while
//! keyless rows, soft-deleted rows, other tenants, and other creators stay
//! unconstrained. This is the cross-process guarantee the in-process locks
//! cannot provide — if the DDL
//! or the entity JSON field names drift, this test fails before production
//! silently loses dedup.

use serde::{Deserialize, Serialize};
use univers_aip_contracts_data::storage::RepositoryTrait;
use univers_aip_contracts_operation::task::{
    TASK_IDEMPOTENCY_INDEX_SQL, TASK_LEGACY_OPEN_BRIDGE_INDEX_SQL, TASK_OPEN_IDEMPOTENCY_INDEX_SQL,
};
use univers_aip_lib_storage_sqlite::{ensure_index_best_effort, open_in_memory, SqliteRepository};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MiniTask {
    #[allow(dead_code)]
    id: String,
    #[allow(dead_code)]
    version: i64,
    #[allow(dead_code)]
    created_by: String,
    #[allow(dead_code)]
    idempotency_key: Option<String>,
    #[allow(dead_code)]
    open_idempotency_key: Option<String>,
    #[allow(dead_code)]
    idempotency_contract_digest: Option<String>,
    #[allow(dead_code)]
    status: String,
    #[allow(dead_code)]
    deleted_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MiniTaskData {
    version: i64,
    organization_id: Option<String>,
    created_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    open_idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    idempotency_contract_digest: Option<String>,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    deleted_at: Option<String>,
}

fn row(
    organization: Option<&str>,
    creator: &str,
    key: Option<&str>,
    deleted: bool,
) -> MiniTaskData {
    MiniTaskData {
        version: 1,
        organization_id: organization.map(str::to_string),
        created_by: creator.to_string(),
        idempotency_key: key.map(str::to_string),
        open_idempotency_key: None,
        idempotency_contract_digest: None,
        status: "pending".to_string(),
        deleted_at: deleted.then(|| "2026-01-01T00:00:00Z".to_string()),
    }
}

fn open_row(request_key: &str, incident_key: &str, status: &str) -> MiniTaskData {
    MiniTaskData {
        version: 1,
        organization_id: Some("org-a".to_string()),
        created_by: "service:automation".to_string(),
        idempotency_key: Some(request_key.to_string()),
        open_idempotency_key: Some(incident_key.to_string()),
        idempotency_contract_digest: Some("sha256:new-open".to_string()),
        status: status.to_string(),
        deleted_at: None,
    }
}

fn lifetime_row(request_key: &str) -> MiniTaskData {
    MiniTaskData {
        version: 1,
        organization_id: Some("org-a".to_string()),
        created_by: "service:automation".to_string(),
        idempotency_key: Some(request_key.to_string()),
        open_idempotency_key: None,
        idempotency_contract_digest: Some("sha256:new-lifetime".to_string()),
        status: "pending".to_string(),
        deleted_at: None,
    }
}

#[tokio::test]
async fn index_enforces_one_live_row_per_tenant_creator_and_key() {
    let pool = open_in_memory().await.expect("pool");
    assert!(
        ensure_index_best_effort(&pool, "tasks", TASK_IDEMPOTENCY_INDEX_SQL, "test").await,
        "index must apply on a fresh store"
    );
    let repo: SqliteRepository<MiniTask, MiniTaskData> =
        SqliteRepository::new(pool.clone(), "tasks");

    repo.create_with_id("t1", row(Some("org-a"), "user:alice", Some("K"), false))
        .await
        .expect("first keyed row insert");
    // Same creator + same key → the index must refuse the duplicate,
    // and the error must carry the index name (the service layer greps
    // for it to map the race to an idempotent replay).
    let err = repo
        .create_with_id("t2", row(Some("org-a"), "user:alice", Some("K"), false))
        .await
        .expect_err("duplicate (creator, key) must be rejected");
    assert!(
        err.to_string()
            .contains(univers_aip_contracts_operation::task::TASK_IDEMPOTENCY_INDEX_NAME)
            || err.to_string().to_lowercase().contains("unique"),
        "constraint error must be recognisable: {err}"
    );

    // Different tenant, same creator and key → allowed (tenant isolation).
    repo.create_with_id("t3", row(Some("org-b"), "user:alice", Some("K"), false))
        .await
        .expect("other tenant may reuse the creator and key");
    // Different creator, same tenant and key → allowed (per-principal scoping, the
    // F-series confidentiality fix's storage twin).
    repo.create_with_id("t4", row(Some("org-a"), "user:mallory", Some("K"), false))
        .await
        .expect("other creator may reuse the key");
    // Missing legacy tenant values normalize to `system` and remain fenced.
    repo.create_with_id("t5", row(None, "user:legacy", Some("K"), false))
        .await
        .expect("first legacy tenant row");
    repo.create_with_id("t6", row(None, "user:legacy", Some("K"), false))
        .await
        .expect_err("legacy tenant rows must share the system fence");
    // Keyless rows → unconstrained.
    repo.create_with_id("t7", row(Some("org-a"), "user:alice", None, false))
        .await
        .expect("keyless row unconstrained");
    repo.create_with_id("t8", row(Some("org-a"), "user:alice", None, false))
        .await
        .expect("second keyless row unconstrained");
    // Soft-deleted rows leave the constraint (partial WHERE) — a
    // deleted task's key is reusable by its creator.
    repo.create_with_id("t9", row(Some("org-a"), "user:alice", Some("K2"), true))
        .await
        .expect("soft-deleted keyed row");
    repo.create_with_id("t10", row(Some("org-a"), "user:alice", Some("K2"), false))
        .await
        .expect("live row may reuse a soft-deleted row's key");
}

#[tokio::test]
async fn lifetime_and_open_indexes_fence_distinct_authority_keys() {
    let pool = open_in_memory().await.expect("pool");
    assert!(ensure_index_best_effort(&pool, "tasks", TASK_IDEMPOTENCY_INDEX_SQL, "lifetime").await);
    assert!(
        ensure_index_best_effort(
            &pool,
            "tasks",
            TASK_OPEN_IDEMPOTENCY_INDEX_SQL,
            "open incident",
        )
        .await
    );
    assert!(
        ensure_index_best_effort(
            &pool,
            "tasks",
            TASK_LEGACY_OPEN_BRIDGE_INDEX_SQL,
            "legacy-open bridge",
        )
        .await
    );
    let repo: SqliteRepository<MiniTask, MiniTaskData> =
        SqliteRepository::new(pool.clone(), "tasks");

    repo.create_with_id("open-1", open_row("request-1", "incident-a", "pending"))
        .await
        .expect("first open incident");
    let open_error = repo
        .create_with_id("open-2", open_row("request-2", "incident-a", "pending"))
        .await
        .expect_err("different request keys cannot create two open incident owners");
    assert!(
        open_error
            .to_string()
            .contains(univers_aip_contracts_operation::task::TASK_OPEN_IDEMPOTENCY_INDEX_NAME)
            || open_error.to_string().to_lowercase().contains("unique")
    );

    let lifetime_error = repo
        .create_with_id("open-3", open_row("request-1", "incident-b", "pending"))
        .await
        .expect_err("one request key cannot bind two Tasks");
    assert!(
        lifetime_error
            .to_string()
            .contains(univers_aip_contracts_operation::task::TASK_IDEMPOTENCY_INDEX_NAME)
            || lifetime_error.to_string().to_lowercase().contains("unique")
    );

    repo.create_with_id(
        "terminal-1",
        open_row("request-3", "incident-terminal", "completed"),
    )
    .await
    .expect("terminal incident leaves the open fence");
    repo.create_with_id(
        "terminal-next",
        open_row("request-4", "incident-terminal", "pending"),
    )
    .await
    .expect("one next incident is admitted after terminal");

    repo.create_with_id("lifetime-independent", lifetime_row("incident-a"))
        .await
        .expect("digested lifetime key stays outside the legacy-open bridge");
}

#[tokio::test]
async fn legacy_single_key_open_row_conflicts_with_new_open_writer() {
    let pool = open_in_memory().await.expect("pool");
    for (ddl, context) in [
        (TASK_IDEMPOTENCY_INDEX_SQL, "lifetime"),
        (TASK_OPEN_IDEMPOTENCY_INDEX_SQL, "open"),
        (TASK_LEGACY_OPEN_BRIDGE_INDEX_SQL, "legacy-open bridge"),
    ] {
        assert!(ensure_index_best_effort(&pool, "tasks", ddl, context).await);
    }
    let repo: SqliteRepository<MiniTask, MiniTaskData> =
        SqliteRepository::new(pool.clone(), "tasks");
    repo.create_with_id(
        "legacy-open",
        row(
            Some("org-a"),
            "service:automation",
            Some("incident-legacy"),
            false,
        ),
    )
    .await
    .expect("legacy ambiguous open row");
    let error = repo
        .create_with_id(
            "new-open",
            open_row("request-new", "incident-legacy", "pending"),
        )
        .await
        .expect_err("bridge must fence old and new open writers");
    assert!(
        error
            .to_string()
            .contains(univers_aip_contracts_operation::task::TASK_LEGACY_OPEN_BRIDGE_INDEX_NAME)
            || error.to_string().to_lowercase().contains("unique")
    );
}

#[test]
fn postgres_contract_has_separate_lifetime_and_open_predicates() {
    let lifetime = univers_aip_contracts_operation::task::TASK_IDEMPOTENCY_INDEX_POSTGRES_SQL;
    let open = univers_aip_contracts_operation::task::TASK_OPEN_IDEMPOTENCY_INDEX_POSTGRES_SQL;
    let bridge = univers_aip_contracts_operation::task::TASK_LEGACY_OPEN_BRIDGE_INDEX_POSTGRES_SQL;
    assert!(lifetime.contains("data->>'idempotency_key'"));
    assert!(!lifetime.contains("open_idempotency_key"));
    assert!(open.contains("data->>'open_idempotency_key'"));
    for status in ["pending", "assigned", "in_progress", "blocked"] {
        assert!(open.contains(status));
    }
    assert!(!open.contains("CASE"));
    assert!(bridge.contains("COALESCE(data->>'open_idempotency_key'"));
    assert!(bridge.contains("idempotency_contract_digest' IS NULL"));
    assert!(bridge.contains("data->>'idempotency_key' IS NOT NULL"));
    for status in ["pending", "assigned", "in_progress", "blocked"] {
        assert!(bridge.contains(status));
    }
}

#[tokio::test]
async fn index_creation_is_best_effort_over_legacy_duplicates() {
    let pool = open_in_memory().await.expect("pool");
    let repo: SqliteRepository<MiniTask, MiniTaskData> =
        SqliteRepository::new(pool.clone(), "tasks");
    // Pre-existing duplicates (legacy data) BEFORE the index exists.
    repo.create_with_id("d1", row(Some("org-a"), "user:alice", Some("K"), false))
        .await
        .expect("legacy row 1");
    repo.create_with_id("d2", row(Some("org-a"), "user:alice", Some("K"), false))
        .await
        .expect("legacy row 2 — no index yet, allowed");
    // Boot must not crash: creation fails, returns false, system continues.
    assert!(
        !ensure_index_best_effort(&pool, "tasks", TASK_IDEMPOTENCY_INDEX_SQL, "test").await,
        "index over duplicate rows must fail soft, not panic"
    );
}
