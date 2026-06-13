// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for RC8: `id(r)` on a relationship lowers to `r._vid` inside `WHERE`.
//!
//! `RETURN id(r)` is correct (df_expr resolves an edge's id to the `_eid`
//! column), but the WHERE-clause predicate-pushdown path
//! (`uni-query/src/query/planner.rs` `rewrite_id_to_vid` /
//! `metadata_function_column`) unconditionally rewrites `id(x)` to `x._vid`,
//! ignoring whether `x` is an edge. So `WHERE id(r) = <eid>` compares the edge id
//! against the wrong column and silently returns the wrong rows.
//!
//! To keep the repro independent of internal id numbering, the source node `a`
//! is created *second* so its `_vid` cannot coincide with the first edge's
//! `_eid`; under the bug the WHERE filter (on `_vid == eid`) therefore misses the
//! edge entirely and the match count drops to zero.
//!
//! Fixed by making the WHERE-clause `id()` rewrite edge-aware (emit `_eid` for a
//! relationship binding); now a regression guard.
//!
//! Run with:
//!   cargo nextest run -p uni --test integration bug_edge_id_in_where

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// `WHERE id(r) = <edge id>` matches exactly the edge with that id.
#[tokio::test]
async fn where_id_on_relationship_uses_edge_id() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("N")
        .property("name", DataType::String)
        .done()
        .edge_type("LINK", &["N"], &["N"])
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    // Create `b` first, `a` second: `a`'s vid cannot equal the edge's eid.
    tx.execute("CREATE (:N {name: 'b'})").await?;
    tx.execute("CREATE (:N {name: 'a'})").await?;

    // Capture the edge id via the working `RETURN id(r)` path.
    let eid = tx
        .query_with(
            "MATCH (a:N {name: 'a'}), (b:N {name: 'b'}) \
             CREATE (a)-[r:LINK]->(b) RETURN id(r) AS eid",
        )
        .fetch_all()
        .await?
        .rows()[0]
        .get::<i64>("eid")?;
    tx.commit().await?;

    // `WHERE id(r) = $eid` must select exactly that edge, and `id(r)` in the
    // projection must echo the same id back.
    let result = session
        .query_with("MATCH ()-[r:LINK]->() WHERE id(r) = $eid RETURN id(r) AS got")
        .param("eid", Value::Int(eid))
        .fetch_all()
        .await?;
    assert_eq!(
        result.rows().len(),
        1,
        "WHERE id(r) = eid must match exactly the edge with that id"
    );
    assert_eq!(
        result.rows()[0].get::<i64>("got")?,
        eid,
        "the matched edge's id must equal the queried edge id"
    );

    Ok(())
}
