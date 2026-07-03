// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression test for issue #134: a dense `similar_to` scan must return the
//! same results whether or not the scanned rows also carry an unread wide
//! `List(Vector)` (multivector) column.
//!
//! The bug was a projection-pushdown leak: `RETURN id(n), similar_to(...)`
//! marked `n` as needing all properties, so the wide ColBERT column was read
//! from Lance and decoded for every row even though the query never references
//! it (~60x slowdown). The fix stops `id(n)` from widening `n` to `*`.
//!
//! This test pins the *correctness* half of the fix: identical dense data run
//! through the identical query must yield identical results regardless of which
//! unused columns exist on the row. The performance half is asserted
//! deterministically by the planner unit tests in `uni-query`
//! (`pushdown_tests::test_issue_134_*`).

use uni_db::{DataType, QueryResult, Uni, Value};

const DIM: usize = 8;
const COLBERT_TOKENS: usize = 16;
const N_ROWS: usize = 20;

/// Deterministic dense vector for a row/query seed.
fn dense(seed: usize) -> Vec<f32> {
    (0..DIM)
        .map(|i| ((seed * 7 + i * 3) % 11) as f32 / 11.0)
        .collect()
}

/// A wide `List(Vector)` value rendered as a Cypher nested-list literal.
fn colbert_literal(seed: usize) -> String {
    let toks: Vec<String> = (0..COLBERT_TOKENS)
        .map(|t| {
            let v: Vec<String> = (0..DIM)
                .map(|i| format!("{:.4}", ((seed + t + i) % 13) as f32 / 13.0))
                .collect();
            format!("[{}]", v.join(", "))
        })
        .collect();
    format!("[{}]", toks.join(", "))
}

/// `(id, score)` pairs in result order.
fn scored(result: &QueryResult) -> Vec<(i64, f64)> {
    result
        .rows()
        .iter()
        .map(|r| (r.get::<i64>("nid").unwrap(), r.get::<f64>("score").unwrap()))
        .collect()
}

/// Build a fresh DB (optionally with the unread wide column), insert identical
/// dense data, and run the issue #134 query, returning `(id, score)` rows.
async fn run(with_colbert: bool) -> anyhow::Result<Vec<(i64, f64)>> {
    let db = Uni::temporary().build().await?;

    let mut schema = db
        .schema()
        .label("Obs")
        .property("embedding", DataType::Vector { dimensions: DIM });
    if with_colbert {
        schema = schema.property(
            "colbert",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        );
    }
    schema.apply().await?;

    let tx = db.session().tx().await?;
    for seed in 0..N_ROWS {
        let emb: Vec<String> = dense(seed).iter().map(|x| format!("{x:.4}")).collect();
        let cypher = if with_colbert {
            format!(
                "CREATE (:Obs {{embedding: [{}], colbert: {}}})",
                emb.join(", "),
                colbert_literal(seed)
            )
        } else {
            format!("CREATE (:Obs {{embedding: [{}]}})", emb.join(", "))
        };
        tx.execute(&cypher).await?;
    }
    tx.commit().await?;
    db.flush().await?;

    let qvec: Vec<f32> = dense(3);
    let result = db
        .session()
        .query_with(
            "MATCH (n:Obs) \
             RETURN id(n) AS nid, similar_to([n.embedding], [$qvec]) AS score \
             ORDER BY score DESC, nid ASC LIMIT 10",
        )
        .param("qvec", Value::Vector(qvec))
        .fetch_all()
        .await?;

    Ok(scored(&result))
}

#[tokio::test]
async fn issue_134_wide_column_does_not_change_dense_results() -> anyhow::Result<()> {
    let dense_only = run(false).await?;
    let dense_plus_colbert = run(true).await?;

    assert!(!dense_only.is_empty(), "query should return rows");
    assert_eq!(
        dense_only, dense_plus_colbert,
        "dense similar_to results must be identical with and without the unread colbert column"
    );
    Ok(())
}
