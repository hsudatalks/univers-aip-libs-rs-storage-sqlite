//! Integration tests for `SqliteGraphRepository` against an in-memory SQLite DB.
//!
//! Each test gets its own isolated in-memory database via `open_in_memory()`.

use serde_json::json;
use univers_aip_contracts_data::storage::graph_repository::{
    RelationProjectionFence, RelationProjectionMutation,
};
use univers_aip_contracts_data::storage::GraphRepository;
use univers_aip_lib_storage_sqlite::{open_in_memory, SqliteGraphRepository};

async fn setup() -> SqliteGraphRepository {
    let pool = open_in_memory().await.expect("open in-memory pool");
    SqliteGraphRepository::new(pool)
        .await
        .expect("create graph repository")
}

async fn assert_newer_owner_survives_stale_delete(
    repo: &SqliteGraphRepository,
    stale_owner: RelationProjectionFence,
) {
    let replacement_fence = RelationProjectionFence {
        owner: "relation/ref-2".to_string(),
        revision: 3,
    };
    assert_eq!(
        repo.upsert_relation_projection(
            "tasks",
            "t1",
            "agents",
            "a1",
            "assigned_to",
            std::collections::HashMap::from([("payload".to_string(), json!("v2"))]),
            replacement_fence.clone(),
        )
        .await
        .unwrap(),
        RelationProjectionMutation::Applied
    );
    assert_eq!(
        repo.delete_relation_projection("tasks", "t1", "agents", "a1", "assigned_to", stale_owner,)
            .await
            .unwrap(),
        RelationProjectionMutation::OwnershipMismatch {
            current: replacement_fence
        }
    );
    assert!(repo
        .exists("tasks", "t1", "agents", "a1", "assigned_to")
        .await
        .unwrap());
}

#[tokio::test]
async fn fenced_projection_retains_delete_revision_and_rejects_legacy_edges() {
    let repo = setup().await;
    let metadata = std::collections::HashMap::from([("payload".to_string(), json!("v1"))]);
    let fence = |revision| RelationProjectionFence {
        owner: "relation/ref-1".to_string(),
        revision,
    };

    assert_eq!(
        repo.upsert_relation_projection(
            "tasks",
            "t1",
            "agents",
            "a1",
            "assigned_to",
            metadata.clone(),
            fence(1),
        )
        .await
        .unwrap(),
        RelationProjectionMutation::Applied
    );
    assert_eq!(
        repo.upsert_relation_projection(
            "tasks",
            "t1",
            "agents",
            "a1",
            "assigned_to",
            metadata.clone(),
            fence(1),
        )
        .await
        .unwrap(),
        RelationProjectionMutation::AlreadyApplied
    );
    assert_eq!(
        repo.upsert_relation_projection(
            "tasks",
            "t1",
            "agents",
            "a1",
            "assigned_to",
            metadata.clone(),
            RelationProjectionFence {
                owner: "relation/ref-2".to_string(),
                revision: 1,
            },
        )
        .await
        .unwrap(),
        RelationProjectionMutation::OwnershipMismatch { current: fence(1) }
    );
    assert_eq!(
        repo.delete_relation_projection("tasks", "t1", "agents", "a1", "assigned_to", fence(2),)
            .await
            .unwrap(),
        RelationProjectionMutation::Applied
    );
    assert!(!repo
        .exists("tasks", "t1", "agents", "a1", "assigned_to")
        .await
        .unwrap());
    assert_eq!(
        repo.upsert_relation_projection(
            "tasks",
            "t1",
            "agents",
            "a1",
            "assigned_to",
            metadata,
            fence(1),
        )
        .await
        .unwrap(),
        RelationProjectionMutation::Superseded { current: fence(2) }
    );
    assert_newer_owner_survives_stale_delete(&repo, fence(4)).await;

    repo.relate("tasks", "legacy", "agents", "a1", "assigned_to")
        .await
        .unwrap();
    assert!(repo
        .upsert_relation_projection(
            "tasks",
            "legacy",
            "agents",
            "a1",
            "assigned_to",
            std::collections::HashMap::new(),
            fence(1),
        )
        .await
        .is_err());
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn legacy_mutators_fail_closed_for_projection_live_rows_and_tombstones() {
    let repo = setup().await;
    let fence = RelationProjectionFence {
        owner: "relation/ref-1".to_string(),
        revision: 1,
    };
    let edge = (
        "tasks".to_string(),
        "hub".to_string(),
        "agents".to_string(),
        "a1".to_string(),
        "assigned_to".to_string(),
    );
    repo.upsert_relation_projection(
        &edge.0,
        &edge.1,
        &edge.2,
        &edge.3,
        &edge.4,
        std::collections::HashMap::new(),
        fence.clone(),
    )
    .await
    .unwrap();
    repo.relate("tasks", "hub", "agents", "legacy", "observes")
        .await
        .unwrap();

    assert!(repo
        .relate_with_metadata(&edge.0, &edge.1, &edge.2, &edge.3, &edge.4, json!({}))
        .await
        .is_err());
    assert!(repo
        .update_relation_metadata(&edge.0, &edge.1, &edge.2, &edge.3, &edge.4, json!({}))
        .await
        .is_err());
    assert!(repo
        .unrelate(&edge.0, &edge.1, &edge.2, &edge.3, &edge.4)
        .await
        .is_err());
    assert!(repo.unrelate_all("tasks", "hub", &[]).await.is_err());
    assert!(repo.sweep_entity("tasks", "hub").await.is_err());
    assert!(repo
        .relate_batch(vec![
            (
                "tasks".into(),
                "new".into(),
                "agents".into(),
                "a2".into(),
                "observes".into(),
            ),
            edge.clone(),
        ])
        .await
        .is_err());
    assert!(!repo
        .exists("tasks", "new", "agents", "a2", "observes")
        .await
        .unwrap());
    assert!(repo
        .unrelate_batch(vec![
            (
                "tasks".into(),
                "hub".into(),
                "agents".into(),
                "legacy".into(),
                "observes".into(),
            ),
            edge.clone(),
        ])
        .await
        .is_err());
    assert!(repo
        .exists("tasks", "hub", "agents", "legacy", "observes")
        .await
        .unwrap());

    repo.delete_relation_projection(
        &edge.0,
        &edge.1,
        &edge.2,
        &edge.3,
        &edge.4,
        RelationProjectionFence {
            owner: fence.owner,
            revision: 2,
        },
    )
    .await
    .unwrap();
    assert!(repo
        .relate_with_metadata(&edge.0, &edge.1, &edge.2, &edge.3, &edge.4, json!({}))
        .await
        .is_err());
    assert!(repo
        .unrelate(&edge.0, &edge.1, &edge.2, &edge.3, &edge.4)
        .await
        .is_err());
}

#[tokio::test]
async fn relate_is_idempotent_and_unrelate_is_noop_safe() {
    let repo = setup().await;

    // First relate creates the edge.
    assert!(repo
        .relate("tasks", "t1", "agents", "a1", "assigned_to")
        .await
        .unwrap());
    // Second relate is a no-op and reports "already existed".
    assert!(!repo
        .relate("tasks", "t1", "agents", "a1", "assigned_to")
        .await
        .unwrap());
    assert!(repo
        .exists("tasks", "t1", "agents", "a1", "assigned_to")
        .await
        .unwrap());

    // Forward and reverse lookups.
    assert_eq!(
        repo.find_targets("tasks", "t1", "assigned_to")
            .await
            .unwrap(),
        vec![("agents".to_string(), "a1".to_string())]
    );
    assert_eq!(
        repo.find_sources("agents", "a1", "assigned_to")
            .await
            .unwrap(),
        vec![("tasks".to_string(), "t1".to_string())]
    );

    // Unrelate removes it; repeating is an idempotent no-op.
    repo.unrelate("tasks", "t1", "agents", "a1", "assigned_to")
        .await
        .unwrap();
    repo.unrelate("tasks", "t1", "agents", "a1", "assigned_to")
        .await
        .unwrap();
    assert!(!repo
        .exists("tasks", "t1", "agents", "a1", "assigned_to")
        .await
        .unwrap());
}

#[tokio::test]
async fn metadata_roundtrip_and_update_missing_edge_errors() {
    let repo = setup().await;

    repo.relate_with_metadata(
        "tasks",
        "t1",
        "agents",
        "a1",
        "assigned_to",
        json!({"weight": 3, "note": "primary"}),
    )
    .await
    .unwrap();

    let targets = repo
        .find_targets_with_metadata("tasks", "t1", "assigned_to")
        .await
        .unwrap();
    assert_eq!(targets.len(), 1);
    let (ref pair, ref meta) = targets[0];
    assert_eq!(pair, &("agents".to_string(), "a1".to_string()));
    let meta = meta.as_ref().expect("metadata present");
    assert_eq!(meta.get("weight"), Some(&json!(3)));

    // Replace metadata on the existing edge.
    repo.update_relation_metadata(
        "tasks",
        "t1",
        "agents",
        "a1",
        "assigned_to",
        json!({"weight": 9}),
    )
    .await
    .unwrap();
    let sources = repo
        .find_sources_with_metadata("agents", "a1", "assigned_to")
        .await
        .unwrap();
    assert_eq!(
        sources[0].1.as_ref().unwrap().get("weight"),
        Some(&json!(9))
    );

    // Updating a non-existent edge is NotFound.
    let err = repo
        .update_relation_metadata("tasks", "missing", "agents", "a1", "assigned_to", json!({}))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        univers_aip_contracts_data::storage::GraphRepositoryError::NotFound(_)
    ));
}

#[tokio::test]
async fn batch_sweep_pagination_and_stats() {
    let repo = setup().await;

    let edges: Vec<(String, String, String, String, String)> = (0..5)
        .map(|i| {
            (
                "tasks".to_string(),
                "hub".to_string(),
                "agents".to_string(),
                format!("a{i}"),
                "assigned_to".to_string(),
            )
        })
        .collect();
    // Batch create; re-running creates nothing new.
    assert_eq!(repo.relate_batch(edges.clone()).await.unwrap(), 5);
    assert_eq!(repo.relate_batch(edges).await.unwrap(), 0);
    // Incoming edge so the sweep covers both directions.
    assert!(repo
        .relate("orgs", "o1", "tasks", "hub", "owns")
        .await
        .unwrap());

    // Deterministic pagination over the targets.
    let page = repo
        .find_targets_paginated("tasks", "hub", "assigned_to", 2, 2)
        .await
        .unwrap();
    assert_eq!(
        page,
        vec![
            ("agents".to_string(), "a2".to_string()),
            ("agents".to_string(), "a3".to_string()),
        ]
    );

    // Stats: filtered and unfiltered.
    let stats = repo.get_stats(&[]).await.unwrap();
    assert_eq!(stats.total, 6);
    assert_eq!(stats.by_type.get("assigned_to"), Some(&5));
    assert_eq!(stats.by_source_table.get("orgs"), Some(&1));
    let filtered = repo.get_stats(&["owns"]).await.unwrap();
    assert_eq!(filtered.total, 1);

    // Sweep removes every edge touching the hub (both directions).
    assert_eq!(repo.sweep_entity("tasks", "hub").await.unwrap(), 6);
    assert_eq!(repo.get_stats(&[]).await.unwrap().total, 0);
}
