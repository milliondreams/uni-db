// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end runtime repro for uni-query finding [25]
//! (`crates/uni-query/src/query/df_graph/locy_query.rs:138`).
//!
//! # Bug
//!
//! `apply_return_clause` implements `RETURN DISTINCT` by deduplicating rows on
//! `format!("{row:?}")`, where each row is a `FactRow` = `HashMap<String,
//! Value>`. Each projected row is a freshly-constructed `HashMap`, and the
//! `Debug` rendering iterates the map in its per-instance (randomized) order.
//! Two rows with byte-identical content therefore render to DIFFERENT debug
//! strings whenever their maps happen to iterate their keys in a different
//! order, so the `HashSet<String>` dedup key fails to collapse them and the
//! duplicates SURVIVE `DISTINCT`.
//!
//! The failure needs multi-column rows: a single-column `HashMap` has only one
//! iteration order, so its debug string is stable and dedup works. With two or
//! more columns there are multiple possible orders, so a batch of identical
//! duplicate rows splits into one surviving row PER observed iteration order
//! (typically ~2 for two columns) instead of collapsing to exactly one.

use anyhow::Result;
use uni_db::locy::LocyConfig;
use uni_db::{DataType, Uni};

/// Number of byte-identical duplicate rows fed through `RETURN DISTINCT`. With
/// two projected columns each row is a two-entry `HashMap` whose debug order is
/// randomized per instance, so across this many rows at least two distinct
/// orders are seen with overwhelming probability — making >1 survivor a
/// reliable (not flaky) observation of the bug.
const N_DUPLICATES: usize = 48;

fn config() -> LocyConfig {
    LocyConfig {
        max_iterations: 1000,
        ..Default::default()
    }
}

/// Seed `N_DUPLICATES` items that all share the SAME `(color, shape)` pair but
/// have distinct ids (so the rule's KEY yields one fact per item, and the
/// `RETURN DISTINCT color, shape` projection produces N identical two-column
/// rows that DISTINCT must collapse to exactly one).
async fn seed_items(db: &Uni) -> Result<()> {
    db.schema()
        .label("Item")
        .property("id", DataType::Int64)
        .property("color", DataType::String)
        .property("shape", DataType::String)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    for i in 0..N_DUPLICATES {
        tx.execute(&format!(
            "CREATE (:Item {{id: {i}, color: 'red', shape: 'round'}})"
        ))
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

const DISTINCT_QUERY: &str = "CREATE RULE r AS \
     MATCH (a:Item) \
     YIELD KEY a.id AS id, a.color AS color, a.shape AS shape \n\
     QUERY r RETURN DISTINCT color, shape";

/// Regression for finding [25]: `RETURN DISTINCT` over N byte-identical
/// multi-column rows must yield exactly ONE row. Formerly failed because the
/// dedup key was `format!("{row:?}")` over a `HashMap`, whose per-instance
/// `Debug` order let content-identical rows split across keys and survive; the
/// dedup now keys on a sorted `BTreeMap<String, Value>` with `Value`'s canonical
/// `Hash`/`Eq`, so identical rows always collapse.
#[tokio::test]
async fn distinct_multicolumn_duplicates_should_collapse_to_one() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed_items(&db).await?;

    let result = db
        .session()
        .locy_with(DISTINCT_QUERY)
        .with_config(config())
        .run()
        .await?;

    let rows = result.rows().cloned().unwrap_or_default();
    println!(
        "[25] RETURN DISTINCT over {N_DUPLICATES} identical rows -> {} surviving rows",
        rows.len()
    );
    assert_eq!(
        rows.len(),
        1,
        "correct: DISTINCT over identical (color,shape) rows must return exactly one row"
    );
    Ok(())
}
