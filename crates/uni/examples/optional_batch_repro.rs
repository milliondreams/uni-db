//! Verification repro: does OptionalFilterExec emit spurious/duplicate NULL
//! recovery rows when a source group's rows straddle input batch boundaries?
//!
//! Setup: 1 `A` node, 20000 `B` nodes with y=1..20000. The OPTIONAL MATCH
//! pattern `(b:B)` is disconnected from `a`, so the plan is
//! OptionalFilterExec over CrossJoinExec (non-equi predicate blocks the
//! LeftOuter HashJoin rewrite).
//!
//! Q1: WHERE b.y > 19999  -> exactly one B passes. Cypher requires exactly
//!     1 row, no NULL. Bug: extra NULL-padded rows from right batches with
//!     no passing row.
//! Q2: WHERE b.y > 99999  -> no B passes. Cypher requires exactly 1 row with
//!     b NULL. Bug: one NULL row per right batch.

use uni_db::{DataType, Uni, Value};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("A")
        .property("x", DataType::Int)
        .apply()
        .await?;
    db.schema()
        .label("B")
        .property("y", DataType::Int)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:A {x: 0})").await?;
    tx.execute("UNWIND range(1, 20000) AS i CREATE (:B {y: i})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    for (name, pred, expect_rows, expect_nulls) in [
        ("Q1 one-late-pass", "b.y > 19999", 1usize, 0usize),
        ("Q2 none-pass", "b.y > 99999", 1usize, 1usize),
    ] {
        let q =
            format!("MATCH (a:A) OPTIONAL MATCH (b:B) WHERE {pred} RETURN a.x AS ax, b.y AS by");
        let r = db.session().query(&q).await?;
        let rows = r.rows();
        let nulls = rows
            .iter()
            .filter(|row| matches!(row.value("by"), Some(Value::Null) | None))
            .count();
        let non_nulls: Vec<i64> = rows
            .iter()
            .filter_map(|row| row.get::<i64>("by").ok())
            .collect();
        println!(
            "{name}: total_rows={} null_by_rows={} non_null_by={:?} (expected rows={} nulls={})",
            rows.len(),
            nulls,
            &non_nulls[..non_nulls.len().min(10)],
            expect_rows,
            expect_nulls
        );
        if rows.len() != expect_rows || nulls != expect_nulls {
            println!("  -> MISMATCH: OPTIONAL MATCH semantics violated");
        } else {
            println!("  -> OK");
        }
    }

    Ok(())
}
