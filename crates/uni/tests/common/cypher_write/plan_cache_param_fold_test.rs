// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Regression tests for the 2026-06-10 review #1: the plan cache keyed on query
// text + schema version only, but the planner *folds* parameterized
// `LIMIT $n` / `SKIP $n` into the cached plan as concrete values. A second call
// with a different value hit the cache and replayed the first call's bound
// limit/skip — returning the wrong number of rows.
//
// The fix folds the values of any LIMIT/SKIP-bound parameters into the cache
// key, so each distinct value gets its own entry. These tests cover both the
// read path (`Session::execute_cached`) and the transaction write path
// (`UniInner::execute_internal_with_tx_l0`).
// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// Seed `n` `Item` nodes with `idx` 0..n.
async fn seed_items(db: &Uni, n: i64) -> Result<()> {
    db.schema()
        .label("Item")
        .property("idx", DataType::Int64)
        .done()
        .apply()
        .await?;
    let rows: Vec<std::collections::HashMap<String, Value>> = (0..n)
        .map(|i| {
            let mut m = std::collections::HashMap::new();
            m.insert("idx".to_string(), Value::Int(i));
            m
        })
        .collect();
    let tx = db.session().tx().await?;
    tx.bulk_insert_vertices("Item", rows).await?;
    tx.commit().await?;
    Ok(())
}

/// Read path: `LIMIT $n` on a shared (cache-hot) session must honor each call's
/// value, not the first one baked into the cached plan.
#[tokio::test]
async fn read_path_parameterized_limit_not_frozen_by_cache() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed_items(&db, 100).await?;

    let session = db.session();
    let cypher = "MATCH (n:Item) RETURN n LIMIT $n";

    // First call primes the cache with LIMIT folded to 1.
    let r1 = session
        .query_with(cypher)
        .param("n", 1i64)
        .fetch_all()
        .await?;
    assert_eq!(r1.rows().len(), 1, "first call should return 1 row");

    // Second call, same text, different value — must return 5, not the cached 1.
    let r5 = session
        .query_with(cypher)
        .param("n", 5i64)
        .fetch_all()
        .await?;
    assert_eq!(
        r5.rows().len(),
        5,
        "LIMIT $n=5 must return 5 rows, not the cached LIMIT=1"
    );

    // Back to 1 — the original entry must still be correct.
    let r1b = session
        .query_with(cypher)
        .param("n", 1i64)
        .fetch_all()
        .await?;
    assert_eq!(r1b.rows().len(), 1, "LIMIT $n=1 must still return 1 row");
    Ok(())
}

/// Read path: `SKIP $n` is folded the same way and must not freeze.
#[tokio::test]
async fn read_path_parameterized_skip_not_frozen_by_cache() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed_items(&db, 10).await?;

    let session = db.session();
    let cypher = "MATCH (n:Item) RETURN n SKIP $s";

    let r0 = session
        .query_with(cypher)
        .param("s", 0i64)
        .fetch_all()
        .await?;
    assert_eq!(r0.rows().len(), 10, "SKIP 0 returns all 10");

    let r7 = session
        .query_with(cypher)
        .param("s", 7i64)
        .fetch_all()
        .await?;
    assert_eq!(
        r7.rows().len(),
        3,
        "SKIP $s=7 must return 3 rows, not the cached SKIP=0"
    );
    Ok(())
}

/// Read path: a const expression over a parameter (`LIMIT $n + 1`) is folded at
/// plan time too and must vary by the parameter's value.
#[tokio::test]
async fn read_path_parameterized_limit_expr_not_frozen() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed_items(&db, 100).await?;

    let session = db.session();
    let cypher = "MATCH (n:Item) RETURN n LIMIT $n + 1";

    let r2 = session
        .query_with(cypher)
        .param("n", 1i64)
        .fetch_all()
        .await?;
    assert_eq!(r2.rows().len(), 2, "LIMIT $n+1 with n=1 returns 2");

    let r6 = session
        .query_with(cypher)
        .param("n", 5i64)
        .fetch_all()
        .await?;
    assert_eq!(
        r6.rows().len(),
        6,
        "LIMIT $n+1 with n=5 must return 6, not the cached 2"
    );
    Ok(())
}

/// Transaction write path: the same fold happens in
/// `execute_internal_with_tx_l0`; a `LIMIT $n` read inside a tx must not freeze.
#[tokio::test]
async fn tx_path_parameterized_limit_not_frozen_by_cache() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed_items(&db, 100).await?;

    let cypher = "MATCH (n:Item) RETURN n LIMIT $n";

    let tx = db.session().tx().await?;
    let r1 = tx.query_with(cypher).param("n", 1i64).fetch_all().await?;
    assert_eq!(r1.rows().len(), 1, "tx LIMIT $n=1 returns 1");

    let r5 = tx.query_with(cypher).param("n", 5i64).fetch_all().await?;
    assert_eq!(
        r5.rows().len(),
        5,
        "tx LIMIT $n=5 must return 5 rows, not the cached LIMIT=1"
    );
    tx.rollback();
    Ok(())
}
