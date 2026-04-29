//! Integration tests for issue #55 PR #5 — cross-MATCH dynamic VID-filter
//! pushdown via `VidLookupJoinExec`.
//!
//! Pattern targeted:
//!
//! ```cypher
//! MATCH (a:Source) WHERE a.score > 0.5
//! MATCH (b:Target) WHERE id(b) = a.linked_vid
//! RETURN id(a) AS aid, id(b) AS bid
//! ```
//!
//! Pre-fix: full scan of the `Target` label per call. With this PR, the
//! build side (`MATCH (a:Source)`) is materialized at runtime, its
//! `linked_vid` column extracted into an `_vid IN (...)` filter, and the
//! probe scan pushed down to Lance.
//!
//! These tests assert *correctness* (results match a HashJoin plan) and
//! *resilience* (queries that deliberately don't match the pattern still
//! execute correctly via the HashJoinExec fallback).

use uni_db::{DataType, Uni, Value};

async fn setup_db() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Source")
        .property("name", DataType::String)
        .property_nullable("linked_vid", DataType::Int64)
        .property_nullable("score", DataType::Float64)
        .done()
        .label("Target")
        .property("name", DataType::String)
        .done()
        .apply()
        .await
        .unwrap();
    db
}

async fn create_target(db: &Uni, name: &str) -> i64 {
    let session = db.session();
    let tx = session.tx().await.unwrap();
    let r = tx
        .query_with("CREATE (n:Target {name: $n}) RETURN id(n) AS vid")
        .param("n", Value::String(name.to_string()))
        .fetch_all()
        .await
        .unwrap();
    tx.commit().await.unwrap();
    r.rows().first().unwrap().get::<i64>("vid").unwrap()
}

async fn create_source(db: &Uni, name: &str, linked_vid: i64, score: f64) -> i64 {
    let session = db.session();
    let tx = session.tx().await.unwrap();
    let r = tx
        .query_with(
            "CREATE (n:Source {name: $n, linked_vid: $v, score: $s}) RETURN id(n) AS vid",
        )
        .param("n", Value::String(name.to_string()))
        .param("v", Value::Int(linked_vid))
        .param("s", Value::Float(score))
        .fetch_all()
        .await
        .unwrap();
    tx.commit().await.unwrap();
    r.rows().first().unwrap().get::<i64>("vid").unwrap()
}

/// Cross-MATCH inner equi-join `id(b) = a.linked_vid` returns exactly the
/// expected (source, target) pairs. This exercises the
/// `VidLookupJoinExec` rewrite path end-to-end.
#[tokio::test]
async fn cross_match_returns_correct_pairs() {
    let db = setup_db().await;

    // 5 targets, 3 sources each linked to a distinct target. Scores split
    // 2 above the threshold and 1 below — exercises the build-side filter.
    let mut target_vids: Vec<i64> = Vec::with_capacity(5);
    for i in 0..5 {
        target_vids.push(create_target(&db, &format!("t{i}")).await);
    }

    create_source(&db, "s_high_a", target_vids[0], 0.9).await;
    create_source(&db, "s_high_b", target_vids[2], 0.7).await;
    create_source(&db, "s_low",    target_vids[3], 0.1).await;

    let session = db.session();
    let result = session
        .query_with(
            "MATCH (a:Source) WHERE a.score > 0.5 \
             MATCH (b:Target) WHERE id(b) = a.linked_vid \
             RETURN a.name AS aname, b.name AS bname",
        )
        .fetch_all()
        .await
        .unwrap();

    let mut pairs: Vec<(String, String)> = result
        .rows()
        .iter()
        .map(|r| {
            (
                r.get::<String>("aname").unwrap(),
                r.get::<String>("bname").unwrap(),
            )
        })
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("s_high_a".to_string(), "t0".to_string()),
            ("s_high_b".to_string(), "t2".to_string()),
        ]
    );

    db.shutdown().await.unwrap();
}

/// When the build side has zero rows after filtering, the join should
/// return zero rows (not error, not return everything).
#[tokio::test]
async fn cross_match_empty_build_returns_empty() {
    let db = setup_db().await;
    let _t = create_target(&db, "t0").await;
    create_source(&db, "low_score", 0, 0.1).await;

    let session = db.session();
    let result = session
        .query_with(
            "MATCH (a:Source) WHERE a.score > 0.99 \
             MATCH (b:Target) WHERE id(b) = a.linked_vid \
             RETURN id(a) AS aid",
        )
        .fetch_all()
        .await
        .unwrap();

    assert_eq!(result.rows().len(), 0);
    db.shutdown().await.unwrap();
}

/// Build-side keys that don't match any vertex on the probe side are
/// dropped (inner-join semantics). Mismatches must NOT generate spurious
/// rows.
#[tokio::test]
async fn cross_match_unmatched_build_keys_are_dropped() {
    let db = setup_db().await;
    let t0 = create_target(&db, "t0").await;

    create_source(&db, "matches_t0",  t0,           0.9).await;
    create_source(&db, "matches_none", 9_999_999,    0.9).await;

    let session = db.session();
    let result = session
        .query_with(
            "MATCH (a:Source) WHERE a.score > 0.5 \
             MATCH (b:Target) WHERE id(b) = a.linked_vid \
             RETURN a.name AS aname",
        )
        .fetch_all()
        .await
        .unwrap();

    let names: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("aname").unwrap())
        .collect();
    assert_eq!(names, vec!["matches_t0".to_string()]);

    db.shutdown().await.unwrap();
}

/// Reverse pair order: `WHERE a.linked_vid = id(b)` should produce the
/// same result as `id(b) = a.linked_vid`. The classifier swaps the pair
/// to maintain the (left-vars, right-vars) invariant; the pre-check
/// must handle either pair order.
#[tokio::test]
async fn cross_match_reverse_pair_order_works() {
    let db = setup_db().await;
    let t = create_target(&db, "t0").await;
    create_source(&db, "s", t, 1.0).await;

    let session = db.session();
    let result = session
        .query_with(
            "MATCH (a:Source) MATCH (b:Target) \
             WHERE a.linked_vid = id(b) \
             RETURN a.name AS aname, b.name AS bname",
        )
        .fetch_all()
        .await
        .unwrap();

    assert_eq!(result.rows().len(), 1);
    let row = result.rows().first().unwrap();
    assert_eq!(row.get::<String>("aname").unwrap(), "s");
    assert_eq!(row.get::<String>("bname").unwrap(), "t0");
    db.shutdown().await.unwrap();
}

/// Negative test: a query whose pattern doesn't match the rewrite (build
/// expression is computed, not a Column) must still produce correct
/// results via the HashJoinExec fallback. This isn't a behavioral
/// difference for the user — just confirms the fallback isn't broken.
#[tokio::test]
async fn cross_match_computed_build_expression_falls_back() {
    let db = setup_db().await;
    let t = create_target(&db, "t").await;
    // linked_vid stores t-1; the join key is `linked_vid + 1`.
    let _ = create_source(&db, "s", t - 1, 1.0).await;

    let session = db.session();
    let result = session
        .query_with(
            "MATCH (a:Source) MATCH (b:Target) \
             WHERE id(b) = a.linked_vid + 1 \
             RETURN a.name AS aname, b.name AS bname",
        )
        .fetch_all()
        .await
        .unwrap();

    // Expression on the build side should compile to BinaryExpr, not a
    // bare Column — the rewrite returns Ok(None) and HashJoinExec
    // produces the correct result.
    assert_eq!(result.rows().len(), 1);
    db.shutdown().await.unwrap();
}

/// Correctness with multiple sources pointing at the same target. Each
/// source should appear once in the output, joined to that one target.
#[tokio::test]
async fn cross_match_many_to_one_join() {
    let db = setup_db().await;
    let t = create_target(&db, "shared").await;

    for i in 0..10 {
        create_source(&db, &format!("s{i}"), t, 1.0).await;
    }

    let session = db.session();
    let result = session
        .query_with(
            "MATCH (a:Source) MATCH (b:Target) \
             WHERE id(b) = a.linked_vid \
             RETURN a.name AS aname",
        )
        .fetch_all()
        .await
        .unwrap();

    let mut names: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("aname").unwrap())
        .collect();
    names.sort();
    let expected: Vec<String> = (0..10).map(|i| format!("s{i}")).collect();
    assert_eq!(names, expected);

    db.shutdown().await.unwrap();
}
