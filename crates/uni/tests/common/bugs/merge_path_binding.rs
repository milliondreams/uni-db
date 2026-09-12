// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `MERGE p = (a)-[:R]->(b)` must bind and measure `p` like `MATCH` does.
//!
//! Two separate defects met here, and the first hid the second.
//!
//! **`length()` measured a path's JSON shape.** `cypher_size_scalar` had arms
//! for strings, lists, maps, nodes and edges, but none for a path, so a path
//! fell to the catch-all that renders it as `{nodes, relationships}` and
//! returns that object's *key count*. Every path, of every length, measured 2.
//! Plausible enough to survive: a one-hop path has two nodes, and 2 came back.
//!
//! **A matched MERGE dropped the relationship from `p`.** The relationship fast
//! path's match branch binds no edge — the relationship is anonymous, so
//! nothing lands in the row — and `bind_path_variables` pushes an edge only for
//! a relationship with a bound variable while requiring just
//! `!nodes.is_empty()`. So `p` came back with its nodes and no relationships.
//! The fast path now declines a pattern carrying a path variable and leaves it
//! to the general path, which materialises the match rows.
//!
//! The first defect masked the second: comparing `length(p)` across the created
//! and matched arms showed 2 and 2, which reads as "no discrepancy" when the
//! underlying paths differ by an entire relationship.

use anyhow::Result;
use uni_db::Uni;

async fn two_nodes() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL E (uid STRING)").await?;
    tx.execute("CREATE EDGE TYPE R FROM E TO E").await?;
    tx.execute("CREATE (:E {uid: 'a'}), (:E {uid: 'b'})")
        .await?;
    tx.commit().await?;
    Ok(db)
}

// Measured with `length` alone. `relationships(p)` would say the same thing
// more directly, but it returns Null on this path — a separate gap, and
// folding it in would make a failure here ambiguous between the two.
// `length` suffices now that it counts relationships: a match branch that
// dropped the edge reports 0 where MATCH reports 1.
const MERGE_PATH: &str = "MATCH (a:E {uid:'a'}), (b:E {uid:'b'}) \
                          MERGE p = (a)-[:R]->(b) \
                          RETURN length(p) AS len";

#[tokio::test]
async fn a_merged_path_measures_and_binds_like_a_matched_one() -> Result<()> {
    let db = two_nodes().await?;

    // First run creates the relationship, second matches it. Both bind `p`.
    let mut seen = Vec::new();
    for _ in 0..2 {
        let tx = db.session().tx().await?;
        let r = tx.query(MERGE_PATH).await?;
        tx.commit().await?;
        seen.push(r.rows()[0].get::<i64>("len")?);
    }

    // The same path bound by MATCH, which never goes through MERGE's binding.
    let reference_rows = db
        .session()
        .query("MATCH p = (:E {uid:'a'})-[:R]->(:E {uid:'b'}) RETURN length(p) AS len")
        .await?;
    let reference = reference_rows.rows()[0].get::<i64>("len")?;

    eprintln!(
        "length(p): created={}  matched={}  match-reference={reference}",
        seen[0], seen[1]
    );

    assert_eq!(
        reference, 1,
        "the MATCH reference itself is wrong, so the comparison below is \
         meaningless: a one-hop path has length 1"
    );
    assert_eq!(
        seen[0], reference,
        "a MERGE that CREATED the relationship bound or measured `p` \
         differently from MATCH"
    );
    assert_eq!(
        seen[1], reference,
        "a MERGE that MATCHED the relationship bound or measured `p` \
         differently from MATCH — the match branch dropped the relationship"
    );

    // Exactly one relationship exists: the second run matched, it did not
    // create a second one.
    let n: i64 = db
        .session()
        .query("MATCH ()-[e:R]->() RETURN count(e) AS n")
        .await?
        .rows()[0]
        .get("n")?;
    assert_eq!(n, 1, "the second MERGE created a duplicate relationship");
    Ok(())
}

/// `length()` counts relationships, on a longer path as well as a one-hop.
///
/// Guards the arm directly. A one-hop path measuring 2 was the original
/// symptom, and a two-hop path measures 2 as well — so the one-hop case alone
/// could be "fixed" by any change that happens to return 1, and the two-hop
/// case is what pins it to the relationship count rather than a coincidence.
#[tokio::test]
async fn length_of_a_path_counts_relationships() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL E (uid STRING)").await?;
    tx.execute("CREATE EDGE TYPE R FROM E TO E").await?;
    tx.execute(
        "CREATE (a:E {uid:'a'}), (b:E {uid:'b'}), (c:E {uid:'c'}), \
         (a)-[:R]->(b), (b)-[:R]->(c)",
    )
    .await?;
    tx.commit().await?;

    for (cypher, want) in [
        (
            "MATCH p = (:E {uid:'a'})-[:R]->(:E {uid:'b'}) RETURN length(p) AS n",
            1,
        ),
        (
            "MATCH p = (:E {uid:'a'})-[:R]->()-[:R]->(:E {uid:'c'}) RETURN length(p) AS n",
            2,
        ),
    ] {
        let got: i64 = db.session().query(cypher).await?.rows()[0].get("n")?;
        assert_eq!(
            got, want,
            "length() returned {got}, want {want}, for: {cypher}"
        );
    }
    Ok(())
}
