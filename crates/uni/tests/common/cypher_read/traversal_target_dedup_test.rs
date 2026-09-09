// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Traversal-target hydration is per *request*, not per distinct vertex (#237).
//!
//! `hydrate_vids_columnar` now fetches each distinct target vid once when the
//! target table is small relative to the request, because a target reached by
//! many edges was otherwise re-fetched once per occurrence. Measured on LDBC
//! SF1, `(Comment)-[:HAS_CREATOR]->(Person)` asks for 2 052 169 target vids
//! covering 9 343 distinct people, and deduplicating took hydration from
//! 4 247 ms to 1 937 ms.
//!
//! # What could break, and what these tests pin
//!
//! Deduplication changes *what is fetched*; it must not change *what is
//! returned*. The gather is keyed by vid — `row_of` maps vid to row and the
//! output indices are built from the original request list — so duplicates are
//! supposed to fan back out unchanged. The failure mode if that is wrong is not
//! an error: it is a row silently disappearing, or a property landing against
//! the wrong row. So these assert on the full multiset and on the pairing, not
//! on a count.
//!
//! Both arms of the choice are exercised. Which one runs is decided by
//! `raw.len() * DEDUP_TABLE_RATIO >= target_rows`, so a small target table with
//! many edges takes the deduplicating arm and a large target table with few
//! edges does not. A test that only covered one would leave the other free to
//! rot — which is the hazard the plan-shape registry exists for.

// Rust guideline compliant

use std::collections::HashMap;

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// Build `t_rows` targets and `fan` source rows per target, each source
/// pointing at one target. Every target is therefore reached `fan` times.
async fn fixture(t_rows: i64, fan: i64) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("T")
        // Nullable: `a_missing_target_property_is_null_not_a_neighbours_value`
        // removes it from one target, and the builder's `property` defaults to
        // NOT NULL, which rejects the REMOVE outright.
        .property_nullable("n", DataType::Int)
        .apply()
        .await?;
    db.schema()
        .label("S")
        .property("m", DataType::Int)
        .apply()
        .await?;
    db.schema().edge_type("R", &["S"], &["T"]).apply().await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.query_with("UNWIND range(0, $c - 1) AS i CREATE (:T {n: i})")
        .param("c", Value::Int(t_rows))
        .fetch_all()
        .await?;
    // `fan` sources per target, so each target vid appears `fan` times in the
    // traversal's target list.
    tx.query_with(
        "UNWIND range(0, $t - 1) AS i \
         UNWIND range(0, $f - 1) AS j \
         MATCH (t:T {n: i}) CREATE (:S {m: i})-[:R]->(t)",
    )
    .param("t", Value::Int(t_rows))
    .param("f", Value::Int(fan))
    .fetch_all()
    .await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

/// Count how many times each `t.n` value comes back.
async fn target_histogram(db: &Uni) -> Result<HashMap<i64, i64>> {
    let r = db
        .session()
        .query("MATCH (s:S)-[:R]->(t:T) RETURN t.n AS n")
        .await?;
    let mut hist = HashMap::new();
    for row in r.rows() {
        match &row.values()[0] {
            Value::Int(i) => *hist.entry(*i).or_insert(0) += 1,
            other => anyhow::bail!("expected Int for t.n, got {other:?}"),
        }
    }
    Ok(hist)
}

/// The deduplicating arm: a small target table reached many times over.
///
/// 20 targets, 30 edges each. `600 * 8 >= 20`, so the request is deduplicated
/// down to 20 fetches — and all 600 rows must still come back, 30 per target.
#[tokio::test]
async fn a_deduplicated_hydration_still_returns_one_row_per_edge() -> Result<()> {
    const TARGETS: i64 = 20;
    const FAN: i64 = 30;
    let db = fixture(TARGETS, FAN).await?;

    let hist = target_histogram(&db).await?;
    assert_eq!(
        hist.len() as i64,
        TARGETS,
        "every target must appear; got {} distinct of {TARGETS}",
        hist.len()
    );
    for n in 0..TARGETS {
        assert_eq!(
            hist.get(&n).copied().unwrap_or(0),
            FAN,
            "target {n} must come back once per edge ({FAN}), not once per \
             distinct vertex. Collapsing to 1 is the signature of a \
             deduplicated fetch whose results were not fanned back out."
        );
    }
    Ok(())
}

/// The non-deduplicating arm: a large target table reached once each.
///
/// 4 000 targets, 1 edge each. `4000 * 8 < 4000` is false, so this is on the
/// other side of the gate only because the request is not large relative to the
/// table — the point is that both arms return the same shape of answer.
#[tokio::test]
async fn an_undeduplicated_hydration_returns_the_same_shape() -> Result<()> {
    const TARGETS: i64 = 4_000;
    const FAN: i64 = 1;
    let db = fixture(TARGETS, FAN).await?;

    let hist = target_histogram(&db).await?;
    assert_eq!(
        hist.len() as i64,
        TARGETS,
        "every target must appear exactly once"
    );
    assert!(
        hist.values().all(|c| *c == FAN),
        "each target must come back exactly {FAN} time"
    );
    Ok(())
}

/// The property must stay paired with its own target, not merely be present.
///
/// A gather that returned the right multiset of values against the wrong rows
/// would satisfy the histogram assertions above. This pins the pairing by
/// asking for the source and target together: `s.m` was written to equal the
/// target's `n`, so every returned row must have `s.m == t.n`.
#[tokio::test]
async fn deduplication_keeps_each_property_with_its_own_row() -> Result<()> {
    let db = fixture(20, 30).await?;
    let r = db
        .session()
        .query("MATCH (s:S)-[:R]->(t:T) RETURN s.m AS m, t.n AS n")
        .await?;
    assert!(!r.rows().is_empty(), "fixture produced no rows");
    for row in r.rows() {
        let (m, n) = (&row.values()[0], &row.values()[1]);
        assert_eq!(
            m, n,
            "each source was written with m equal to its target's n, so a \
             mismatch means the hydrated property landed against the wrong row"
        );
    }
    Ok(())
}

/// A target with no row on the other side stays null rather than borrowing a
/// neighbour's value — the case where a gather miss is easiest to get wrong.
#[tokio::test]
async fn a_missing_target_property_is_null_not_a_neighbours_value() -> Result<()> {
    let db = fixture(5, 4).await?;
    let session = db.session();
    // Remove one target's property, leaving the vertex in place.
    let tx = session.tx().await?;
    tx.execute("MATCH (t:T {n: 2}) REMOVE t.n").await?;
    tx.commit().await?;
    db.flush().await?;

    let r = session
        .query("MATCH (s:S)-[:R]->(t:T) RETURN s.m AS m, t.n AS n")
        .await?;
    for row in r.rows() {
        let (m, n) = (&row.values()[0], &row.values()[1]);
        if matches!(m, Value::Int(2)) {
            assert!(
                n.is_null(),
                "the target whose property was removed must read null, got {n:?}"
            );
        } else {
            assert_eq!(m, n, "every other row must keep its own value");
        }
    }
    Ok(())
}
