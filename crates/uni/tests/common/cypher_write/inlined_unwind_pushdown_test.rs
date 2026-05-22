// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! E2E regression: `UNWIND [{nid: ..., ...}, ...] AS u MATCH (n:L)
//! WHERE id(n) = u.nid SET ...` must push the UNWIND's `nid` values into
//! the scan as a `_vid IN (...)` filter — same behavior as the param
//! form `UNWIND $list AS u`.
//!
//! Before the fix at df_planner.rs's `materialize_unwind_source_field`
//! Expr::List branch, the inlined-literal form bailed and emitted a
//! CrossJoin → Filter over a full GraphScanExec scan of all label rows.
//! Unit tests in `crates/uni-query/src/query/df_planner.rs` pin the
//! exact code path; this test pins the end-user contract via `.profile()`.

use anyhow::Result;
use std::collections::HashMap;
use uni_db::{DataType, Uni, Value};

/// With N=200 Entity rows pre-loaded and an inlined `UNWIND` of 3 map
/// literals, the SET must touch only the 3 targeted vertices. Pre-fix,
/// `GraphScanExec.actual_rows` was 200 (full scan); post-fix it should
/// be at most a small constant (the IN-list-filtered scan output).
#[tokio::test]
async fn inlined_unwind_map_literals_push_to_scan_filter() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property_nullable("frequency", DataType::Int64)
        .property_nullable("confidence", DataType::Float64)
        .done()
        .apply()
        .await?;

    // Bulk-insert 200 Entity rows so a full scan is clearly distinct
    // from a pushed-down 3-row lookup.
    const N: usize = 200;
    let vids: Vec<i64> = {
        let tx = db.session().tx().await?;
        let rows: Vec<uni_common::Properties> = (0..N)
            .map(|i| {
                let mut h: HashMap<String, Value> = HashMap::new();
                h.insert("entity_id".into(), Value::String(format!("e{i}")));
                h.insert("frequency".into(), Value::Int(0));
                h.insert("confidence".into(), Value::Float(0.0));
                h
            })
            .collect();
        let inserted = tx.bulk_insert_vertices("Entity", rows).await?;
        tx.commit().await?;
        inserted.into_iter().map(|v| v.as_u64() as i64).collect()
    };

    // Inlined UNWIND with 3 map literals. Pre-fix, the planner fails
    // to extract the nid values at plan time and falls back to full
    // scan + post-join Filter.
    let v0 = vids[10];
    let v1 = vids[100];
    let v2 = vids[150];

    let tx = db.session().tx().await?;
    let (_res, profile) = tx
        .execute_with(&format!(
            "UNWIND [{{nid: {v0}, f: 7, c: 0.7}}, {{nid: {v1}, f: 8, c: 0.8}}, {{nid: {v2}, f: 9, c: 0.9}}] AS u \
             MATCH (n:Entity) WHERE id(n) = u.nid \
             SET n.frequency = u.f, n.confidence = u.c"
        ))
        .profile()
        .await?;
    tx.commit().await?;

    // Find the GraphScanExec operator and assert its output is bounded.
    // If pushdown failed (the bug), this is N=200; if pushdown fired
    // it's at most a handful of rows (IN-list applied at scan time).
    let scan_op = profile
        .runtime_stats
        .iter()
        .find(|op| op.operator == "GraphScanExec")
        .expect("plan should contain a GraphScanExec");

    assert!(
        scan_op.actual_rows <= 10,
        "expected GraphScanExec to emit at most 10 rows after pushdown, \
         got {} rows out of {N} — the IN-list pushdown for inlined \
         UNWIND of map literals is broken (see df_planner.rs \
         `materialize_unwind_source_field`'s Expr::List branch)",
        scan_op.actual_rows
    );

    // Sanity-check that the SET actually applied to the 3 targeted rows.
    let r = db
        .session()
        .query(&format!(
            "MATCH (n:Entity) WHERE id(n) IN [{v0}, {v1}, {v2}] \
             RETURN n.frequency AS f ORDER BY f"
        ))
        .await?;
    assert_eq!(r.rows().len(), 3);
    assert_eq!(r.rows()[0].get::<i64>("f")?, 7);

    Ok(())
}
