// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression repro for Bug #6 (C2 pinned-tx case): a named graph
//! projection is keyed on a *storage-pointer-specific* registry, so a
//! projection registered against the live `StorageManager` is invisible
//! to any procedure that runs against the transaction's *pinned*
//! `StorageManager`.
//!
//! Root cause: `crates/uni-query/src/projection_store.rs::for_storage`
//! keys the process-global projection registry on
//! `Arc::as_ptr(storage) as usize`. With SSI default-ON, a read-write
//! transaction pins a *separate* `StorageManager`
//! (`transaction.rs` → `db.storage.pinned_at_version(...)`, a distinct
//! `Arc` pointer).
//!
//! Trigger asymmetry (verified): `uni.graph.{project,exists,list,drop}`
//! are NOT on the DataFusion allowlist
//! (`read.rs::is_df_eligible_procedure`), so they run on the fallback
//! simple-executor path against the *live* `self.storage`. But
//! `uni.algo.*` IS allowlisted, so it runs on the DataFusion path against
//! `graph_ctx.storage()` — which, inside an SSI tx, is the *pinned*
//! manager. So an algorithm invoked against a named projection from
//! inside an SSI transaction looks the projection up under the pinned
//! pointer and cannot find it.
//!
//! `algo_against_named_projection_on_session_works` is the control (no
//! pinned storage → the algo finds the projection). `algo_against_named_
//! projection_in_tx_is_invisible` is RED today: the in-tx algo fails with
//! `0x822 no projection named` because it consulted the pinned-storage
//! registry slot.

// Rust guideline compliant

use uni_db::{DataType, Uni};

/// Build an in-memory `Uni` (SSI default-ON) seeded with a committed
/// `Person`/`KNOWS` triangle, then register a Native projection named
/// `g` against the *live* session (live `StorageManager`).
async fn db_with_named_projection() -> anyhow::Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .done()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(
        "CREATE (a:Person {name: 'a'})-[:KNOWS]->(b:Person {name: 'b'}), \
         (b)-[:KNOWS]->(c:Person {name: 'c'}), (c)-[:KNOWS]->(a)",
    )
    .await?;
    tx.commit().await?;
    // Land the committed rows in L1 so the projection builder materialises
    // a non-empty graph (mirrors `named_projection.rs`'s `flush_to_l1`).
    db.flush().await?;

    // Register the projection on the live session: `uni.graph.project`
    // runs on the simple-executor path against the live `StorageManager`,
    // so the registry entry is keyed on the live pointer.
    let projected = session
        .query(
            "CALL uni.graph.project('g', \
             {nodeLabels: ['Person'], edgeTypes: ['KNOWS']}, {}) \
             YIELD name, node_count RETURN name, node_count",
        )
        .await?;
    assert_eq!(
        projected.rows()[0]
            .value("node_count")
            .and_then(uni_db::Value::as_i64),
        Some(3),
        "projection on the live session should see the 3 committed Person nodes"
    );
    Ok(db)
}

/// CONTROL: running `uni.algo.pageRank({name: 'g'})` on the live session
/// (no pinned storage) finds the projection, because the algo's
/// `for_storage(host.storage())` resolves to the same live pointer the
/// projection was registered under.
#[tokio::test]
async fn algo_against_named_projection_on_session_works() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let db = db_with_named_projection().await?;

    let r = db
        .session()
        .query(
            "CALL uni.algo.pageRank({name: 'g'}, {}) \
             YIELD nodeId, score RETURN nodeId, score",
        )
        .await?;
    assert_eq!(
        r.rows().len(),
        3,
        "pageRank on the live session must see the named projection"
    );
    Ok(())
}

/// RED: running the *same* `uni.algo.pageRank({name: 'g'})` from inside
/// an SSI read-write transaction fails. `uni.algo.*` is DataFusion-
/// eligible, so it runs against the transaction's pinned
/// `StorageManager`; `for_storage(pinned_ptr)` is a *different* registry
/// slot than the live one the projection was registered under, so the
/// lookup misses with `0x822 no projection named`.
///
/// This assertion FAILS today (Bug #6, C2 pinned-tx case). When the bug
/// is fixed — projections keyed per-`Database` rather than per-storage-
/// pointer — the in-tx algo must find `g` and return 3 rows.
#[tokio::test]
async fn algo_against_named_projection_in_tx_is_invisible() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let db = db_with_named_projection().await?;

    let session = db.session();
    let tx = session.tx().await?;
    let result = tx
        .query(
            "CALL uni.algo.pageRank({name: 'g'}, {}) \
             YIELD nodeId, score RETURN nodeId, score",
        )
        .await;

    match result {
        Ok(r) => {
            assert_eq!(
                r.rows().len(),
                3,
                "BUG #6: pageRank inside an SSI tx must see the named projection \
                 registered on the live session"
            );
        }
        Err(e) => {
            panic!(
                "BUG #6: projection `g` is invisible to an algo run inside an SSI tx \
                 (registry keyed on the tx's pinned-storage pointer, not the live \
                 StorageManager). Got: {e}"
            );
        }
    }
    tx.commit().await?;
    Ok(())
}
