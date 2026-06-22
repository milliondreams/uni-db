// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Review L6 + L7 — schema-name safety and the fork-local-id invariant.
//!
//! L6: label/edge-type names with characters unsafe for on-disk dataset
//! paths and Lance branch names are rejected — at schema definition, and as
//! a fork-create backstop for names that entered via the infallible
//! schemaless interning path.
//!
//! L7: a fork-origin label/edge-type id can collide with a primary id
//! allocated after the fork point; this is benign because promote and
//! storage resolve by NAME, never by the numeric id.

// Rust guideline compliant

use anyhow::Result;
use uni_db::api::fork_diff::PromotePattern;
use uni_db::{DataType, Uni};

/// L6: explicit `db.schema().label(...)` / `.edge_type(...)` reject unsafe
/// names up front.
#[tokio::test]
async fn schema_definition_rejects_unsafe_names() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    assert!(
        db.schema().label("bad/name").apply().await.is_err(),
        "a label with '/' must be rejected at definition"
    );
    assert!(
        db.schema().label("bad name").apply().await.is_err(),
        "a label with whitespace must be rejected at definition"
    );
    // A benign name with a dot is accepted.
    db.schema().label("My.Label").apply().await?;
    db.shutdown().await?;
    Ok(())
}

/// L6: a name interned schemalessly (bypassing the `add_edge_type`
/// validation) is rejected with a CLEAN error at table creation rather than
/// panicking Lance with an invalid dataset/branch path.
#[tokio::test]
async fn schemaless_unsafe_name_errors_cleanly() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();

    // Schemaless: the backticked edge type interns via the infallible
    // `get_or_assign_edge_type_id` path. The unsafe `deltas_bad/type_fwd`
    // table name is caught at creation (on commit/flush), not at Lance.
    let result: Result<()> = async {
        let tx = session.tx().await?;
        tx.execute("CREATE (a:A)-[:`bad/type`]->(b:B)").await?;
        tx.commit().await?;
        db.flush().await?;
        Ok(())
    }
    .await;

    let err = result.expect_err("an unsafe schemaless name must error, not succeed");
    assert!(
        err.to_string().contains("unsafe character"),
        "expected a clean unsafe-name error, got: {err}"
    );

    db.shutdown().await?;
    Ok(())
}

/// L7: a fork-origin label id colliding with a later primary id is benign —
/// promote resolves by name, so both labels coexist correctly on primary.
#[tokio::test]
async fn fork_origin_id_collision_is_benign() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;

    // Fork mints a new label `Alpha` in its overlay (overlay max+1 → id N).
    // The overlay label is property-less; the schemaless write carries `v`.
    let fork = primary.fork("f").await?;
    fork.fork_schema().label("Alpha").apply().await?;
    let ftx = fork.tx().await?;
    ftx.execute("CREATE (:Alpha {v: 1})").await?;
    ftx.commit().await?;

    // Primary independently mints `Beta` — it never saw `Alpha`, so the
    // per-view `max+1` hands `Beta` the SAME numeric id `Alpha` got in the
    // fork overlay (the L7 collision).
    db.schema()
        .label("Beta")
        .property("v", DataType::Int64)
        .apply()
        .await?;
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Beta {v: 2})").await?;
    tx.commit().await?;

    // Declare `Alpha` on primary too (it gets a fresh, distinct primary id —
    // NOT the fork's colliding one). Promote resolves by NAME, so the id
    // collision is irrelevant and both labels coexist with the right rows.
    db.schema()
        .label("Alpha")
        .property("v", DataType::Int64)
        .apply()
        .await?;
    db.promote_from_fork("f", &[PromotePattern::label("Alpha")])
        .await?;

    let alpha: Vec<i64> = primary
        .query("MATCH (n:Alpha) RETURN n.v AS v")
        .await?
        .rows()
        .iter()
        .filter_map(|r| r.get::<i64>("v").ok())
        .collect();
    let beta: Vec<i64> = primary
        .query("MATCH (n:Beta) RETURN n.v AS v")
        .await?
        .rows()
        .iter()
        .filter_map(|r| r.get::<i64>("v").ok())
        .collect();
    assert_eq!(
        alpha,
        vec![1],
        "Alpha promoted by name despite the id collision"
    );
    assert_eq!(beta, vec![2], "Beta is a distinct label, unaffected");

    drop(fork);
    db.shutdown().await?;
    Ok(())
}
